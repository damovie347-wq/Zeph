package com.fasttransfer.p2p.transfer

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Build
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.os.Process
import android.system.Os
import com.fasttransfer.p2p.model.IncomingTransferRequest
import com.fasttransfer.p2p.model.P2pGroupCredentials
import com.fasttransfer.p2p.model.TransferHistoryRecord
import com.fasttransfer.p2p.model.TransferItem
import com.fasttransfer.p2p.model.TransferProgress
import com.fasttransfer.p2p.model.TransferStatus
import com.fasttransfer.p2p.util.CompatibilityHelper
import com.fasttransfer.p2p.util.StorageHelper
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.RandomAccessFile
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.StandardSocketOptions
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class ZeroCopyTransferEngine(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val getLocalDeviceModel: () -> String,
    private val onPrepareP2pGroup: () -> P2pGroupCredentials?,
    private val onJoinP2pGroup: (P2pGroupCredentials) -> Boolean,
    private val isHighBandUpgradeEnabled: () -> Boolean,
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onIncomingConsentRequest: (IncomingTransferRequest) -> Unit,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit,
    private val onTransferActiveStateChanged: (Boolean) -> Unit
) {
    companion object {
        const val SOCKET_BUFFER_BYTES = 16 * 1024 * 1024 // 16 MB Kernel TCP Window per channel
        const val DIRECT_BUFFER_BYTES = 8 * 1024 * 1024 // 8 MB Direct Off-Heap DMA Buffer
        const val STRIPE_SIZE_BYTES = 4L * 1024L * 1024L // 4 MB Dynamic Parallel Offset Stripe Size
        const val PARALLEL_STREAMS = 4 // 4 Concurrent NIO SocketChannels + pwrite64 Offset Writers
        const val PARALLEL_THRESHOLD_BYTES = 8L * 1024L * 1024L // Use 4-channel striping for files >= 8 MB
        private const val PROTOCOL_MAGIC = 0x5A455039 // "ZEP9" Control & Handshake Channel
        private const val PROTOCOL_MAGIC_TURBO_DATA = 0x5A455054 // "ZEPT" Parallel Worker Data Channel
        private const val DECISION_ACCEPTED: Int = 1
        private const val DECISION_REJECTED: Int = 2
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverJob: Job? = null
    private var httpServerJob: Job? = null
    private var activeTransferJob: Job? = null

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    private val isPaused = AtomicBoolean(false)

    @Volatile
    private var currentCancelFlag = AtomicBoolean(false)

    @Volatile
    var autoAcceptIncoming: Boolean = false

    @Volatile
    private var activeSenderChannel: SocketChannel? = null

    private val activeSenderWorkers: MutableList<SocketChannel> =
        Collections.synchronizedList(mutableListOf())

    @Volatile
    private var activeReceiverChannel: SocketChannel? = null

    private val activeReceiverWorkers: MutableList<SocketChannel> =
        Collections.synchronizedList(mutableListOf())

    private val incomingWorkerQueue = ArrayBlockingQueue<SocketChannel>(16)

    @Volatile
    private var activeConsentLatch: CountDownLatch? = null

    val historyRecords: MutableList<TransferHistoryRecord> =
        Collections.synchronizedList(mutableListOf())

    @Volatile
    private var sharedLinkItems: List<TransferItem> = emptyList()

    fun startServer() {
        startIndestructibleZeroCopyServer()
        startHttpBrowserShareServer()
    }

    fun setItemsForWebLinkShare(items: List<TransferItem>) {
        sharedLinkItems = items
    }

    fun clearHistory() {
        historyRecords.clear()
    }

    fun togglePauseResume(): Boolean {
        val nextState = !isPaused.get()
        isPaused.set(nextState)
        return nextState
    }

    fun isCurrentlyPaused(): Boolean = isPaused.get()

    private fun acquireInProcessHardwareLocks() {
        try {
            val pm = context.applicationContext.getSystemService(Context.POWER_SERVICE) as? PowerManager
            if (pm != null && wakeLock == null) {
                wakeLock = pm.newWakeLock(
                    PowerManager.PARTIAL_WAKE_LOCK,
                    "Zeph::DirectTransferWakeLock"
                ).apply {
                    setReferenceCounted(false)
                    acquire(20 * 60 * 1000L)
                }
            }
        } catch (_: Throwable) {
        }
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null && wifiLock == null) {
                wifiLock = wm.createWifiLock(
                    CompatibilityHelper.getOptimalWifiLockMode(),
                    "Zeph::DirectTransferWifiLock"
                ).apply {
                    setReferenceCounted(false)
                    acquire()
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun releaseInProcessHardwareLocks() {
        try {
            wakeLock?.let { if (it.isHeld) it.release() }
            wakeLock = null
        } catch (_: Throwable) {
        }
        try {
            wifiLock?.let { if (it.isHeld) it.release() }
            wifiLock = null
        } catch (_: Throwable) {
        }
    }

    /**
     * Called ONLY when the user explicitly taps "İptal et" / "Kapat":
     * Immediately sends TCP RST via SO_LINGER(true, 0) on control + all parallel worker channels.
     */
    fun cancelAndPreservePartialFile() {
        currentCancelFlag.set(true)
        isPaused.set(false)
        activeConsentLatch?.countDown()

        val sChan = activeSenderChannel
        val rChan = activeReceiverChannel
        activeSenderChannel = null
        activeReceiverChannel = null

        val sWorkers = ArrayList(activeSenderWorkers)
        activeSenderWorkers.clear()
        val rWorkers = ArrayList(activeReceiverWorkers)
        activeReceiverWorkers.clear()

        scope.launch {
            for (w in sWorkers) abortChannelWithRst(w)
            for (w in rWorkers) abortChannelWithRst(w)
            abortChannelWithRst(sChan)
            abortChannelWithRst(rChan)
            while (true) {
                val queued = incomingWorkerQueue.poll() ?: break
                abortChannelWithRst(queued)
            }
        }
    }

    private fun abortChannelWithRst(ch: SocketChannel?) {
        if (ch == null) return
        try {
            val sock = ch.socket()
            try {
                sock.setSoLinger(true, 0)
            } catch (_: Throwable) {
            }
            try {
                sock.shutdownInput()
            } catch (_: Throwable) {
            }
            try {
                sock.shutdownOutput()
            } catch (_: Throwable) {
            }
            try {
                sock.close()
            } catch (_: Throwable) {
            }
        } catch (_: Throwable) {
        }
        try {
            ch.close()
        } catch (_: Throwable) {
        }
    }

    private fun closeChannelGracefully(ch: SocketChannel?) {
        if (ch == null) return
        try {
            ch.socket().shutdownOutput()
        } catch (_: Throwable) {
        }
        try {
            ch.close()
        } catch (_: Throwable) {
        }
    }

    private fun configureChannelSocketBuffers(channel: SocketChannel) {
        try {
            val socket = channel.socket()
            socket.tcpNoDelay = true
            socket.keepAlive = true
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            socket.receiveBufferSize = SOCKET_BUFFER_BYTES
            // 0xA0 (DSCP CS5 -> UP=5 -> 802.11e WMM AC_VI High-Priority Video TXOP Burst Queue)
            socket.trafficClass = 0xA0
        } catch (_: Throwable) {
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            try {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true)
                channel.setOption(StandardSocketOptions.SO_SNDBUF, SOCKET_BUFFER_BYTES)
                channel.setOption(StandardSocketOptions.SO_RCVBUF, SOCKET_BUFFER_BYTES)
                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
            } catch (_: Throwable) {
            }
        }
    }

    private fun readFully(channel: SocketChannel, buf: ByteBuffer): Boolean {
        while (buf.hasRemaining()) {
            val r = channel.read(buf)
            if (r < 0) return false
        }
        return true
    }

    private fun writeFully(channel: SocketChannel, buf: ByteBuffer): Boolean {
        while (buf.hasRemaining()) {
            val w = channel.write(buf)
            if (w < 0) return false
        }
        return true
    }

    private fun startIndestructibleZeroCopyServer() {
        serverJob?.cancel()
        serverJob = scope.launch {
            while (isActive) {
                var serverChannel: ServerSocketChannel? = null
                try {
                    Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                    serverChannel = ServerSocketChannel.open()
                    serverChannel.configureBlocking(true)
                    serverChannel.socket().reuseAddress = true
                    serverChannel.socket().receiveBufferSize = SOCKET_BUFFER_BYTES
                    serverChannel.socket().bind(InetSocketAddress(transferPort))

                    while (isActive) {
                        val clientChannel = serverChannel.accept() ?: continue
                        scope.launch {
                            dispatchIncomingChannel(clientChannel)
                        }
                    }
                } catch (_: Throwable) {
                    delay(400L)
                } finally {
                    try {
                        serverChannel?.close()
                    } catch (_: Throwable) {
                    }
                }
            }
        }
    }

    private fun dispatchIncomingChannel(clientChannel: SocketChannel) {
        try {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            clientChannel.configureBlocking(true)
            configureChannelSocketBuffers(clientChannel)

            val magicBuf = ByteBuffer.allocateDirect(4)
            if (!readFully(clientChannel, magicBuf)) {
                closeChannelGracefully(clientChannel)
                return
            }
            magicBuf.flip()
            val magic = magicBuf.int

            if (magic == PROTOCOL_MAGIC_TURBO_DATA) {
                // Parallel Worker Data SocketChannel arrived! Push directly to incomingWorkerQueue.
                if (!incomingWorkerQueue.offer(clientChannel, 2L, TimeUnit.SECONDS)) {
                    closeChannelGracefully(clientChannel)
                }
                return
            }
            if (magic != PROTOCOL_MAGIC) {
                closeChannelGracefully(clientChannel)
                return
            }

            val dis = DataInputStream(clientChannel.socket().getInputStream())
            handleIncomingClientChannel(clientChannel, dis)
        } catch (_: Throwable) {
            closeChannelGracefully(clientChannel)
        }
    }

    private fun handleIncomingClientChannel(clientChannel: SocketChannel, dis: DataInputStream) {
        for (oldW in ArrayList(activeReceiverWorkers)) {
            abortChannelWithRst(oldW)
        }
        activeReceiverWorkers.clear()
        while (true) {
            val stale = incomingWorkerQueue.poll() ?: break
            abortChannelWithRst(stale)
        }
        abortChannelWithRst(activeReceiverChannel)

        val sessionCancel = AtomicBoolean(false)
        currentCancelFlag = sessionCancel
        activeReceiverChannel = clientChannel
        isPaused.set(false)

        try {
            val socket = clientChannel.socket()
            val senderIp = socket.inetAddress?.hostAddress ?: "Bilinmeyen IP"
            val dos = DataOutputStream(socket.getOutputStream())

            // 1. Read Handshake Metadata
            val senderAlias = dis.readUTF()
            val senderModel = dis.readUTF().ifBlank { "Android" }
            var transportLabel = dis.readUTF().ifBlank { "Zero-Copy P2P" }
            val fileCount = dis.readInt()
            val totalBatchBytes = dis.readLong()
            val firstFileName = dis.readUTF()
            val firstFileSize = dis.readLong()
            val previewCount = dis.readInt()
            val previewLines = mutableListOf<String>()
            for (i in 0 until previewCount) {
                previewLines.add(dis.readUTF())
            }

            // 2. If connected over a router IP (not 192.168.49.x or 192.168.43.x), immediately spin up
            // the 5 GHz / SCC Autonomous P2P Group while the user is looking at the consent screen!
            val isAlreadyDirectLink = senderIp.startsWith("192.168.49.") || senderIp.startsWith("192.168.43.")
            val p2pGroupDeferred = CompletableDeferred<P2pGroupCredentials?>()
            if (!isAlreadyDirectLink && isHighBandUpgradeEnabled()) {
                scope.launch {
                    try {
                        val creds = onPrepareP2pGroup()
                        p2pGroupDeferred.complete(creds)
                    } catch (_: Throwable) {
                        p2pGroupDeferred.complete(null)
                    }
                }
            } else {
                p2pGroupDeferred.complete(null)
            }

            // 3. Ask Receiver User for Approval
            var accepted = autoAcceptIncoming
            if (!accepted) {
                val latch = CountDownLatch(1)
                activeConsentLatch = latch
                var userDecision = false

                val req = IncomingTransferRequest(
                    senderAlias = senderAlias,
                    senderModel = senderModel,
                    senderIp = senderIp,
                    transportLabel = transportLabel,
                    fileCount = fileCount,
                    totalBytes = totalBatchBytes,
                    firstFileName = firstFileName,
                    firstFileSize = firstFileSize,
                    filePreviewLines = previewLines.joinToString(10.toChar().toString()),
                    onDecision = { approved ->
                        userDecision = approved
                        latch.countDown()
                    }
                )
                onIncomingConsentRequest(req)

                latch.await(60, TimeUnit.SECONDS)
                activeConsentLatch = null
                accepted = userDecision && !sessionCancel.get()
            }

            if (!accepted) {
                try {
                    dos.writeByte(DECISION_REJECTED)
                    dos.flush()
                } catch (_: Throwable) {
                }
                closeChannelGracefully(clientChannel)
                return
            }

            acquireInProcessHardwareLocks()
            onTransferActiveStateChanged(true)

            // Wait up to 3.8s if the 5 GHz / SCC P2P Group is finishing setup
            val p2pCreds: P2pGroupCredentials? = runBlocking {
                withTimeoutOrNull(3800L) {
                    p2pGroupDeferred.await()
                }
            }

            dos.writeByte(DECISION_ACCEPTED)
            if (p2pCreds != null && !isAlreadyDirectLink) {
                dos.writeBoolean(true)
                dos.writeUTF(p2pCreds.ssid)
                dos.writeUTF(p2pCreds.passphrase)
                dos.writeUTF(p2pCreds.ownerIp)
                dos.writeInt(p2pCreds.freqMhz)
                dos.writeUTF(p2pCreds.bandLabel)
                dos.flush()
            } else {
                dos.writeBoolean(false)
                dos.flush()
            }

            // 4. Read how many Parallel Worker Channels the Sender opened (e.g. 4 channels)
            val expectedWorkerCount = try {
                socket.soTimeout = 10_000
                dis.readInt()
            } catch (_: Throwable) {
                0
            }
            val negotiatedLabel = try {
                dis.readUTF()
            } catch (_: Throwable) {
                transportLabel
            } finally {
                try {
                    socket.soTimeout = 0
                } catch (_: Throwable) {
                }
            }
            if (negotiatedLabel.isNotBlank()) {
                transportLabel = negotiatedLabel
            }

            val workerPool = mutableListOf<SocketChannel>()
            for (idx in 0 until expectedWorkerCount) {
                val worker = incomingWorkerQueue.poll(4000L, TimeUnit.MILLISECONDS) ?: break
                activeReceiverWorkers.add(worker)
                workerPool.add(worker)
            }

            val singleDirectBuf = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            for (fileIdx in 0 until fileCount) {
                if (sessionCancel.get()) break

                val fileName = dis.readUTF()
                val expectedFileSize = dis.readLong()
                val useParallelStripes = dis.readBoolean()

                val targetFile: File = StorageHelper.createDirectDownloadTarget(context, fileName)
                val totalBytesReceivedAtomic = AtomicLong(0L)
                val startTimeNs = System.nanoTime()
                val startWallMs = System.currentTimeMillis()
                val lastUiUpdateMs = AtomicLong(0L)

                var raf: RandomAccessFile? = null
                var fileChannel: FileChannel? = null

                try {
                    raf = RandomAccessFile(targetFile, "rw")
                    fileChannel = raf.channel

                    if (expectedFileSize > 0L) {
                        try {
                            Os.posix_fallocate(raf.fd, 0L, expectedFileSize)
                        } catch (_: Throwable) {
                        }
                    }

                    fileChannel.position(0L)

                    if (useParallelStripes && workerPool.isNotEmpty() && expectedFileSize > 0L) {
                        // 4-CHANNEL PARALLEL POSITIONAL OFFSET WRITING (pwrite64):
                        // Each worker thread reads (stripeOffset, stripeLen) from its dedicated SocketChannel
                        // and writes directly to fileChannel at that exact offset in parallel!
                        val fc = fileChannel
                        val readerThreads = workerPool.mapIndexed { wIdx, wChannel ->
                            Thread {
                                try {
                                    Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                                    val headerBuf = ByteBuffer.allocateDirect(12)
                                    val stripeBuf = ByteBuffer.allocateDirect(STRIPE_SIZE_BYTES.toInt())
                                    val ackBuf = ByteBuffer.allocateDirect(1)

                                    while (!sessionCancel.get()) {
                                        headerBuf.clear()
                                        if (!readFully(wChannel, headerBuf)) break
                                        headerBuf.flip()
                                        val stripeOffset = headerBuf.long
                                        val stripeLen = headerBuf.int

                                        if (stripeOffset < 0L) {
                                            // End-of-file marker for this worker channel! Send 1-byte ACK.
                                            ackBuf.clear()
                                            ackBuf.put(1.toByte())
                                            ackBuf.flip()
                                            writeFully(wChannel, ackBuf)
                                            break
                                        }

                                        var receivedInStripe = 0
                                        while (receivedInStripe < stripeLen && !sessionCancel.get()) {
                                            while (isPaused.get() && !sessionCancel.get()) {
                                                Thread.sleep(80L)
                                            }
                                            if (sessionCancel.get()) break

                                            stripeBuf.clear()
                                            val rem = stripeLen - receivedInStripe
                                            if (rem < stripeBuf.capacity()) {
                                                stripeBuf.limit(rem)
                                            }
                                            val nr = wChannel.read(stripeBuf)
                                            if (nr < 0) break
                                            if (nr == 0) continue

                                            stripeBuf.flip()
                                            var writePos = stripeOffset + receivedInStripe.toLong()
                                            while (stripeBuf.hasRemaining()) {
                                                val nw = fc.write(stripeBuf, writePos)
                                                if (nw > 0) {
                                                    writePos += nw.toLong()
                                                    receivedInStripe += nw
                                                    totalBytesReceivedAtomic.addAndGet(nw.toLong())
                                                }
                                            }

                                            val doneBytes = totalBytesReceivedAtomic.get()
                                            val nowMs = System.currentTimeMillis()
                                            val prevMs = lastUiUpdateMs.get()
                                            if (nowMs - prevMs >= 130L || doneBytes == expectedFileSize) {
                                                if (lastUiUpdateMs.compareAndSet(prevMs, nowMs)) {
                                                    val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                                                    val speedMbps = (doneBytes / (1024.0 * 1024.0)) / elapsedSec
                                                    val remBytes = (expectedFileSize - doneBytes).coerceAtLeast(0L)
                                                    val etaSec = if (speedMbps > 0.01) {
                                                        ((remBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                                                    } else {
                                                        0L
                                                    }
                                                    onProgressUpdate(
                                                        TransferProgress(
                                                            fileName = targetFile.name,
                                                            fileIndex = fileIdx,
                                                            totalFiles = fileCount,
                                                            bytesTransferred = doneBytes,
                                                            totalBytes = expectedFileSize,
                                                            speedMbps = speedMbps,
                                                            etaSeconds = etaSec,
                                                            elapsedMillis = nowMs - startWallMs,
                                                            status = TransferStatus.TRANSFERRING,
                                                            transportLabel = transportLabel,
                                                            isSending = false,
                                                            savedFilePath = targetFile.absolutePath
                                                        )
                                                    )
                                                }
                                            }
                                        }
                                    }
                                } catch (_: Throwable) {
                                }
                            }.apply {
                                name = "Zeph-ParallelOffsetWriter-$wIdx"
                                start()
                            }
                        }

                        for (t in readerThreads) {
                            try {
                                t.join()
                            } catch (_: Throwable) {
                            }
                        }
                    } else {
                        // Single-channel fast path on controlChannel for small files / inline text
                        while (totalBytesReceivedAtomic.get() < expectedFileSize && !sessionCancel.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                Thread.sleep(100L)
                            }
                            if (sessionCancel.get()) break

                            singleDirectBuf.clear()
                            val remaining = expectedFileSize - totalBytesReceivedAtomic.get()
                            if (remaining < singleDirectBuf.capacity()) {
                                singleDirectBuf.limit(remaining.toInt())
                            }

                            val bytesRead = clientChannel.read(singleDirectBuf)
                            if (bytesRead < 0) break
                            if (bytesRead == 0) continue

                            singleDirectBuf.flip()
                            while (singleDirectBuf.hasRemaining()) {
                                totalBytesReceivedAtomic.addAndGet(fileChannel.write(singleDirectBuf).toLong())
                            }

                            val doneBytes = totalBytesReceivedAtomic.get()
                            val nowMs = System.currentTimeMillis()
                            val prevMs = lastUiUpdateMs.get()
                            if (nowMs - prevMs >= 130L || doneBytes == expectedFileSize) {
                                lastUiUpdateMs.set(nowMs)
                                val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                                val speedMbps = (doneBytes / (1024.0 * 1024.0)) / elapsedSec
                                val remainingBytes = (expectedFileSize - doneBytes).coerceAtLeast(0L)
                                val etaSec = if (speedMbps > 0.01) {
                                    ((remainingBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                                } else {
                                    0L
                                }

                                onProgressUpdate(
                                    TransferProgress(
                                        fileName = targetFile.name,
                                        fileIndex = fileIdx,
                                        totalFiles = fileCount,
                                        bytesTransferred = doneBytes,
                                        totalBytes = expectedFileSize,
                                        speedMbps = speedMbps,
                                        etaSeconds = etaSec,
                                        elapsedMillis = nowMs - startWallMs,
                                        status = TransferStatus.TRANSFERRING,
                                        transportLabel = transportLabel,
                                        isSending = false,
                                        savedFilePath = targetFile.absolutePath
                                    )
                                )
                            }
                        }
                    }
                } catch (_: Throwable) {
                } finally {
                    val totalBytesReceived = totalBytesReceivedAtomic.get().coerceAtMost(expectedFileSize)
                    try {
                        if (fileChannel != null && fileChannel.isOpen) {
                            if (totalBytesReceived >= 0L && totalBytesReceived < expectedFileSize) {
                                fileChannel.truncate(totalBytesReceived)
                            }
                            fileChannel.force(true)
                        }
                    } catch (_: Throwable) {
                    }
                    try {
                        fileChannel?.close()
                    } catch (_: Throwable) {
                    }
                    try {
                        raf?.close()
                    } catch (_: Throwable) {
                    }

                    if (!sessionCancel.get() && totalBytesReceived == expectedFileSize) {
                        try {
                            dos.writeLong(totalBytesReceived)
                            dos.flush()
                        } catch (_: Throwable) {
                        }
                    }

                    if (totalBytesReceived > 0L) {
                        StorageHelper.notifyMediaScanner(context, targetFile)
                        val isPartial = totalBytesReceived < expectedFileSize
                        val elapsedMs = (System.currentTimeMillis() - startWallMs).coerceAtLeast(1L)
                        val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                        val avgSpeedMbps = (totalBytesReceived / (1024.0 * 1024.0)) / elapsedSec
                        val pct = if (expectedFileSize > 0L) (totalBytesReceived * 100L) / expectedFileSize else 100L

                        val record = TransferHistoryRecord(
                            fileName = targetFile.name,
                            filePath = targetFile.absolutePath,
                            bytesWritten = totalBytesReceived,
                            totalExpectedBytes = expectedFileSize,
                            elapsedMillis = elapsedMs,
                            avgSpeedMbps = avgSpeedMbps,
                            isPartial = isPartial,
                            timestampMs = System.currentTimeMillis()
                        )
                        historyRecords.add(0, record)
                        onHistoryAdded(record)

                        onProgressUpdate(
                            TransferProgress(
                                fileName = targetFile.name,
                                fileIndex = if (isPartial) fileIdx else fileCount,
                                totalFiles = fileCount,
                                bytesTransferred = totalBytesReceived,
                                totalBytes = expectedFileSize,
                                speedMbps = avgSpeedMbps,
                                etaSeconds = 0L,
                                elapsedMillis = elapsedMs,
                                status = if (isPartial) TransferStatus.PARTIAL_PRESERVED else TransferStatus.COMPLETED,
                                transportLabel = transportLabel,
                                isSending = false,
                                savedFilePath = targetFile.absolutePath,
                                message = if (isPartial) {
                                    "Kısmi Dosya Korundu (%$pct • ${StorageHelper.formatFileSize(totalBytesReceived)})"
                                } else {
                                    "Tamamlandı • Süre: ${formatDurationSec(elapsedMs)} • Ort. Hız: ${String.format("%.1f", avgSpeedMbps)} MB/s"
                                }
                            )
                        )
                    }
                }
            }
        } catch (_: Throwable) {
        } finally {
            releaseInProcessHardwareLocks()
            onTransferActiveStateChanged(false)
            if (activeReceiverChannel === clientChannel) {
                activeReceiverChannel = null
            }
            val rWorkers = ArrayList(activeReceiverWorkers)
            activeReceiverWorkers.clear()
            if (sessionCancel.get()) {
                for (w in rWorkers) abortChannelWithRst(w)
                abortChannelWithRst(clientChannel)
            } else {
                for (w in rWorkers) closeChannelGracefully(w)
                closeChannelGracefully(clientChannel)
            }
        }
    }

    fun sendItemsToPeers(
        items: List<TransferItem>,
        targetHostIps: List<Pair<String, Int>>,
        transportLabel: String
    ) {
        if (items.isEmpty() || targetHostIps.isEmpty()) return

        for (w in ArrayList(activeSenderWorkers)) {
            abortChannelWithRst(w)
        }
        activeSenderWorkers.clear()
        abortChannelWithRst(activeSenderChannel)
        activeTransferJob?.cancel()

        val sessionCancel = AtomicBoolean(false)
        currentCancelFlag = sessionCancel
        isPaused.set(false)

        activeTransferJob = scope.launch {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            acquireInProcessHardwareLocks()
            onTransferActiveStateChanged(true)
            try {
                for ((hostIp, port) in targetHostIps) {
                    if (sessionCancel.get()) break
                    sendItemsToSinglePeer(items, hostIp, port, transportLabel, sessionCancel)
                }
            } finally {
                releaseInProcessHardwareLocks()
                onTransferActiveStateChanged(false)
            }
        }
    }

    private fun sendItemsToSinglePeer(
        items: List<TransferItem>,
        hostIp: String,
        port: Int,
        initialTransportLabel: String,
        sessionCancel: AtomicBoolean
    ) {
        var controlChannel: SocketChannel? = null
        val workerChannels = mutableListOf<SocketChannel>()
        var transportLabel = initialTransportLabel

        try {
            val totalBatchBytes = items.sumOf { it.sizeBytes }
            val firstItem = items.first()
            onProgressUpdate(
                TransferProgress(
                    fileName = firstItem.name,
                    fileIndex = 0,
                    totalFiles = items.size,
                    bytesTransferred = 0L,
                    totalBytes = totalBatchBytes,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    elapsedMillis = 0L,
                    status = TransferStatus.CONNECTING,
                    transportLabel = transportLabel,
                    isSending = true,
                    message = "$hostIp:$port ile bağlantı kuruluyor..."
                )
            )

            controlChannel = SocketChannel.open()
            activeSenderChannel = controlChannel
            controlChannel.configureBlocking(true)
            configureChannelSocketBuffers(controlChannel)
            CompatibilityHelper.bindSocketToOptimalInterface(context, controlChannel.socket(), hostIp)
            controlChannel.socket().connect(InetSocketAddress(hostIp, port), 5000)

            // Write 4-byte PROTOCOL_MAGIC via NIO first so Receiver's dispatchIncomingChannel reads it cleanly
            val magicOut = ByteBuffer.allocateDirect(4)
            magicOut.putInt(PROTOCOL_MAGIC)
            magicOut.flip()
            writeFully(controlChannel, magicOut)

            val dos = DataOutputStream(controlChannel.socket().getOutputStream())
            val dis = DataInputStream(controlChannel.socket().getInputStream())

            // 1. Send Handshake Metadata & Wait for Receiver's Approval
            dos.writeUTF(getLocalAlias())
            dos.writeUTF(getLocalDeviceModel())
            dos.writeUTF(transportLabel)
            dos.writeInt(items.size)
            dos.writeLong(totalBatchBytes)
            dos.writeUTF(firstItem.name)
            dos.writeLong(firstItem.sizeBytes)

            val previewItems = items.take(8)
            dos.writeInt(previewItems.size)
            for (pItem in previewItems) {
                dos.writeUTF("• ${pItem.name} (${StorageHelper.formatFileSize(pItem.sizeBytes)})")
            }
            dos.flush()

            onProgressUpdate(
                TransferProgress(
                    fileName = firstItem.name,
                    fileIndex = 0,
                    totalFiles = items.size,
                    bytesTransferred = 0L,
                    totalBytes = totalBatchBytes,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    elapsedMillis = 0L,
                    status = TransferStatus.WAITING_APPROVAL,
                    transportLabel = transportLabel,
                    isSending = true,
                    message = "Karşı cihazın ($hostIp) 'Kabul et' onayı bekleniyor..."
                )
            )

            val decision = dis.readByte().toInt()
            if (decision != DECISION_ACCEPTED) {
                onProgressUpdate(
                    TransferProgress(
                        fileName = firstItem.name,
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = 0L,
                        totalBytes = totalBatchBytes,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.REJECTED,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = "Karşı cihaz dosya gönderimini reddetti."
                    )
                )
                return
            }

            // 2. Check if Receiver offered an Automatic 5 GHz / SCC Direct P2P Bridge Upgrade!
            val hasP2pUpgrade = dis.readBoolean()
            var effectiveDataHostIp = hostIp

            if (hasP2pUpgrade && !sessionCancel.get()) {
                val p2pSsid = dis.readUTF()
                val p2pPwd = dis.readUTF()
                val p2pOwnerIp = dis.readUTF()
                val p2pFreq = dis.readInt()
                val p2pLabel = dis.readUTF()

                onProgressUpdate(
                    TransferProgress(
                        fileName = firstItem.name,
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = 0L,
                        totalBytes = totalBatchBytes,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.CONNECTING,
                        transportLabel = p2pLabel,
                        isSending = true,
                        message = "Otomatik $p2pLabel tüneline geçiliyor..."
                    )
                )

                val creds = P2pGroupCredentials(
                    ssid = p2pSsid,
                    passphrase = p2pPwd,
                    ownerIp = p2pOwnerIp,
                    freqMhz = p2pFreq,
                    bandLabel = p2pLabel
                )

                val joinedP2p = onJoinP2pGroup(creds)
                if (joinedP2p && !sessionCancel.get()) {
                    effectiveDataHostIp = p2pOwnerIp
                    transportLabel = "$p2pLabel • 4 Kanal Paralel"
                }
            } else if (hostIp.startsWith("192.168.49.")) {
                transportLabel = "$transportLabel • 4 Kanal Paralel"
            }

            // 3. Open 4 Parallel NIO Data Channels to effectiveDataHostIp for files >= 8 MB!
            val needsParallelPool = items.any { it.sizeBytes >= PARALLEL_THRESHOLD_BYTES && it.inlineTextBytes == null }
            if (needsParallelPool && !sessionCancel.get()) {
                for (wIdx in 0 until PARALLEL_STREAMS) {
                    if (sessionCancel.get()) break
                    var connectedWorker: SocketChannel? = null
                    for (attempt in 0 until 8) {
                        if (sessionCancel.get()) break
                        try {
                            val candidate = SocketChannel.open()
                            candidate.configureBlocking(true)
                            configureChannelSocketBuffers(candidate)
                            CompatibilityHelper.bindSocketToOptimalInterface(context, candidate.socket(), effectiveDataHostIp)
                            candidate.socket().connect(InetSocketAddress(effectiveDataHostIp, port), 1200)
                            val workerMagicBuf = ByteBuffer.allocateDirect(4)
                            workerMagicBuf.putInt(PROTOCOL_MAGIC_TURBO_DATA)
                            workerMagicBuf.flip()
                            if (writeFully(candidate, workerMagicBuf)) {
                                connectedWorker = candidate
                                break
                            } else {
                                closeChannelGracefully(candidate)
                            }
                        } catch (_: Throwable) {
                            Thread.sleep(150L)
                        }
                    }
                    if (connectedWorker != null) {
                        activeSenderWorkers.add(connectedWorker)
                        workerChannels.add(connectedWorker)
                    } else if (wIdx == 0 && effectiveDataHostIp != hostIp) {
                        // If P2P interface routing wasn't reachable on first worker, fall back to hostIp for parallel pool
                        effectiveDataHostIp = hostIp
                        transportLabel = "$initialTransportLabel • 4 Kanal Paralel"
                    }
                }
            }

            dos.writeInt(workerChannels.size)
            dos.writeUTF(transportLabel)
            dos.flush()

            val singleDirectBuffer = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            for ((fileIdx, item) in items.withIndex()) {
                if (sessionCancel.get()) break

                var pfd: ParcelFileDescriptor? = null
                var fis: FileInputStream? = null
                var fileChannel: FileChannel? = null

                try {
                    val actualSize: Long
                    if (item.inlineTextBytes != null) {
                        actualSize = item.inlineTextBytes.size.toLong()
                    } else if (item.directFile != null && item.directFile.exists()) {
                        fis = FileInputStream(item.directFile)
                        fileChannel = fis.channel
                        actualSize = item.directFile.length()
                    } else if (item.uri != null) {
                        pfd = context.contentResolver.openFileDescriptor(item.uri, "r")
                        if (pfd != null) {
                            fis = FileInputStream(pfd.fileDescriptor)
                            fileChannel = fis.channel
                            actualSize = if (item.sizeBytes > 0L) item.sizeBytes else fileChannel.size()
                        } else {
                            continue
                        }
                    } else {
                        continue
                    }

                    val useParallelStripes = workerChannels.isNotEmpty() &&
                        actualSize >= PARALLEL_THRESHOLD_BYTES &&
                        item.inlineTextBytes == null &&
                        fileChannel != null

                    dos.writeUTF(item.name)
                    dos.writeLong(actualSize)
                    dos.writeBoolean(useParallelStripes)
                    dos.flush()

                    val totalBytesSentAtomic = AtomicLong(0L)
                    val startTimeNs = System.nanoTime()
                    val startWallMs = System.currentTimeMillis()
                    val lastUiUpdateMs = AtomicLong(0L)

                    if (useParallelStripes && fileChannel != null) {
                        // 4-CHANNEL PARALLEL ZERO-COPY OFFSET STRIPING:
                        // All 4 worker channels dynamically pull 4 MB stripes (0..4MB, 4..8MB, 8..12MB...)
                        // and stream them concurrently via kernel sendfile64 (transferTo) / pread64!
                        val fc = fileChannel
                        val nextStripeOffset = AtomicLong(0L)

                        val senderThreads = workerChannels.mapIndexed { wIdx, wChannel ->
                            Thread {
                                try {
                                    Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                                    val headerBuf = ByteBuffer.allocateDirect(12)
                                    val fallbackBuf = ByteBuffer.allocateDirect(STRIPE_SIZE_BYTES.toInt())
                                    val ackBuf = ByteBuffer.allocateDirect(1)
                                    var zeroCopySupported = true

                                    while (!sessionCancel.get()) {
                                        val stripeOffset = nextStripeOffset.getAndAdd(STRIPE_SIZE_BYTES)
                                        if (stripeOffset >= actualSize) break
                                        val stripeLen = (actualSize - stripeOffset).coerceAtMost(STRIPE_SIZE_BYTES).toInt()

                                        headerBuf.clear()
                                        headerBuf.putLong(stripeOffset)
                                        headerBuf.putInt(stripeLen)
                                        headerBuf.flip()
                                        if (!writeFully(wChannel, headerBuf)) break

                                        var sentInStripe = 0L
                                        while (sentInStripe < stripeLen && !sessionCancel.get()) {
                                            while (isPaused.get() && !sessionCancel.get()) {
                                                onProgressUpdate(
                                                    TransferProgress(
                                                        fileName = item.name,
                                                        fileIndex = fileIdx,
                                                        totalFiles = items.size,
                                                        bytesTransferred = totalBytesSentAtomic.get(),
                                                        totalBytes = actualSize,
                                                        speedMbps = 0.0,
                                                        etaSeconds = 0L,
                                                        elapsedMillis = System.currentTimeMillis() - startWallMs,
                                                        status = TransferStatus.PAUSED,
                                                        transportLabel = transportLabel,
                                                        isSending = true,
                                                        message = "Gönderim Duraklatıldı"
                                                    )
                                                )
                                                Thread.sleep(80L)
                                            }
                                            if (sessionCancel.get()) break

                                            val currentPos = stripeOffset + sentInStripe
                                            val remInStripe = stripeLen.toLong() - sentInStripe

                                            if (zeroCopySupported) {
                                                try {
                                                    val transferred = fc.transferTo(currentPos, remInStripe, wChannel)
                                                    if (transferred > 0L) {
                                                        sentInStripe += transferred
                                                        totalBytesSentAtomic.addAndGet(transferred)
                                                    } else {
                                                        zeroCopySupported = false
                                                    }
                                                } catch (_: Throwable) {
                                                    if (sessionCancel.get()) break
                                                    zeroCopySupported = false
                                                }
                                            }

                                            if (!zeroCopySupported && !sessionCancel.get()) {
                                                fallbackBuf.clear()
                                                if (remInStripe < fallbackBuf.capacity()) {
                                                    fallbackBuf.limit(remInStripe.toInt())
                                                }
                                                val nr = fc.read(fallbackBuf, currentPos)
                                                if (nr <= 0) break
                                                fallbackBuf.flip()
                                                if (!writeFully(wChannel, fallbackBuf)) break
                                                sentInStripe += nr.toLong()
                                                totalBytesSentAtomic.addAndGet(nr.toLong())
                                            }

                                            val doneBytes = totalBytesSentAtomic.get()
                                            val nowMs = System.currentTimeMillis()
                                            val prevMs = lastUiUpdateMs.get()
                                            if (nowMs - prevMs >= 130L) {
                                                if (lastUiUpdateMs.compareAndSet(prevMs, nowMs)) {
                                                    val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                                                    val speedMbps = (doneBytes / (1024.0 * 1024.0)) / elapsedSec
                                                    val remBytes = (actualSize - doneBytes).coerceAtLeast(0L)
                                                    val etaSec = if (speedMbps > 0.01) {
                                                        ((remBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                                                    } else {
                                                        0L
                                                    }
                                                    val reported = if (doneBytes >= actualSize) (actualSize - 1L).coerceAtLeast(0L) else doneBytes
                                                    onProgressUpdate(
                                                        TransferProgress(
                                                            fileName = item.name,
                                                            fileIndex = fileIdx,
                                                            totalFiles = items.size,
                                                            bytesTransferred = reported,
                                                            totalBytes = actualSize,
                                                            speedMbps = speedMbps,
                                                            etaSeconds = etaSec.coerceAtLeast(1L),
                                                            elapsedMillis = nowMs - startWallMs,
                                                            status = TransferStatus.TRANSFERRING,
                                                            transportLabel = transportLabel,
                                                            isSending = true
                                                        )
                                                    )
                                                }
                                            }
                                        }
                                    }

                                    if (!sessionCancel.get()) {
                                        // Send EOF marker (-1L, 0) for this worker and wait for 1-byte worker ACK
                                        headerBuf.clear()
                                        headerBuf.putLong(-1L)
                                        headerBuf.putInt(0)
                                        headerBuf.flip()
                                        if (writeFully(wChannel, headerBuf)) {
                                            ackBuf.clear()
                                            readFully(wChannel, ackBuf)
                                        }
                                    }
                                } catch (_: Throwable) {
                                }
                            }.apply {
                                name = "Zeph-ParallelSender-$wIdx"
                                start()
                            }
                        }

                        for (t in senderThreads) {
                            try {
                                t.join()
                            } catch (_: Throwable) {
                            }
                        }
                    } else if (item.inlineTextBytes != null) {
                        val buf = ByteBuffer.wrap(item.inlineTextBytes)
                        while (buf.hasRemaining() && !sessionCancel.get()) {
                            totalBytesSentAtomic.addAndGet(controlChannel.write(buf).toLong())
                        }
                    } else if (fileChannel != null) {
                        var zeroCopySupported = true
                        while (totalBytesSentAtomic.get() < actualSize && !sessionCancel.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                Thread.sleep(100L)
                            }
                            if (sessionCancel.get()) break

                            val pos = totalBytesSentAtomic.get()
                            if (zeroCopySupported) {
                                try {
                                    val count = (actualSize - pos).coerceAtMost(STRIPE_SIZE_BYTES * 4L)
                                    val transferred = fileChannel.transferTo(pos, count, controlChannel)
                                    if (transferred > 0L) {
                                        totalBytesSentAtomic.addAndGet(transferred)
                                    } else {
                                        zeroCopySupported = false
                                    }
                                } catch (_: Throwable) {
                                    if (sessionCancel.get()) break
                                    zeroCopySupported = false
                                }
                            }

                            if (!zeroCopySupported && !sessionCancel.get()) {
                                singleDirectBuffer.clear()
                                val rem = actualSize - totalBytesSentAtomic.get()
                                if (rem < singleDirectBuffer.capacity()) {
                                    singleDirectBuffer.limit(rem.toInt())
                                }
                                val readCount = fileChannel.read(singleDirectBuffer, totalBytesSentAtomic.get())
                                if (readCount <= 0) break
                                singleDirectBuffer.flip()
                                if (!writeFully(controlChannel, singleDirectBuffer)) break
                                totalBytesSentAtomic.addAndGet(readCount.toLong())
                            }
                        }
                    }

                    var bytesSent = totalBytesSentAtomic.get().coerceAtMost(actualSize)
                    if (!sessionCancel.get() && bytesSent == actualSize) {
                        dos.flush()
                        try {
                            controlChannel.socket().soTimeout = 30_000
                            val confirmedByReceiver = dis.readLong()
                            if (confirmedByReceiver > 0L) {
                                bytesSent = confirmedByReceiver
                            }
                        } catch (_: Throwable) {
                        }
                    }

                    val elapsedMs = (System.currentTimeMillis() - startWallMs).coerceAtLeast(1L)
                    val totalElapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                    val finalSpeedMbps = (bytesSent / (1024.0 * 1024.0)) / totalElapsedSec
                    val isPartial = bytesSent < actualSize
                    val pct = if (actualSize > 0L) (bytesSent * 100L) / actualSize else 100L

                    onProgressUpdate(
                        TransferProgress(
                            fileName = item.name,
                            fileIndex = if (isPartial) fileIdx else items.size,
                            totalFiles = items.size,
                            bytesTransferred = bytesSent,
                            totalBytes = actualSize,
                            speedMbps = finalSpeedMbps,
                            etaSeconds = 0L,
                            elapsedMillis = elapsedMs,
                            status = if (isPartial) TransferStatus.PARTIAL_PRESERVED else TransferStatus.COMPLETED,
                            transportLabel = transportLabel,
                            isSending = true,
                            message = if (isPartial) {
                                "Durduruldu (%$pct gönderildi • Alıcıda kısmi dosya korundu)"
                            } else {
                                "Tamamlandı • Süre: ${formatDurationSec(elapsedMs)} • Ort. Hız: ${String.format("%.1f", finalSpeedMbps)} MB/s"
                            }
                        )
                    )
                } finally {
                    try {
                        fileChannel?.close()
                    } catch (_: Throwable) {
                    }
                    try {
                        fis?.close()
                    } catch (_: Throwable) {
                    }
                    try {
                        pfd?.close()
                    } catch (_: Throwable) {
                    }
                }
            }
        } catch (t: Throwable) {
            if (sessionCancel.get()) {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.firstOrNull()?.name ?: "Dosya",
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = 0L,
                        totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.PARTIAL_PRESERVED,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = "İşlem durduruldu (Kısmi dosya korundu)."
                    )
                )
            } else {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.firstOrNull()?.name ?: "Dosya",
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = 0L,
                        totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.ERROR,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = "Hedef cihaza ($hostIp) ulaşılamadı."
                    )
                )
            }
        } finally {
            if (activeSenderChannel === controlChannel) {
                activeSenderChannel = null
            }
            activeSenderWorkers.removeAll(workerChannels)
            if (sessionCancel.get()) {
                for (w in workerChannels) abortChannelWithRst(w)
                abortChannelWithRst(controlChannel)
            } else {
                for (w in workerChannels) closeChannelGracefully(w)
                closeChannelGracefully(controlChannel)
            }
        }
    }

    private fun formatDurationSec(elapsedMs: Long): String {
        val totalSec = elapsedMs / 1000.0
        if (totalSec < 60.0) {
            return String.format("%.1f sn", totalSec)
        }
        val mins = (totalSec / 60).toInt()
        val secs = (totalSec % 60).toInt()
        return "${mins} dk ${secs} sn"
    }

    private fun startHttpBrowserShareServer() {
        httpServerJob?.cancel()
        httpServerJob = scope.launch {
            while (isActive) {
                var serverSocket: ServerSocket? = null
                try {
                    serverSocket = ServerSocket()
                    serverSocket.reuseAddress = true
                    serverSocket.bind(InetSocketAddress(httpLinkPort))
                    while (isActive) {
                        val client = serverSocket.accept() ?: continue
                        scope.launch {
                            handleHttpClient(client)
                        }
                    }
                } catch (_: Throwable) {
                    delay(500L)
                } finally {
                    try {
                        serverSocket?.close()
                    } catch (_: Throwable) {
                    }
                }
            }
        }
    }

    private fun handleHttpClient(socket: Socket) {
        try {
            socket.tcpNoDelay = true
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            val reader = socket.getInputStream().bufferedReader()
            val requestLine = reader.readLine() ?: return
            val parts = requestLine.split(" ")
            val path = if (parts.size >= 2) parts[1] else "/"

            val out = BufferedOutputStream(socket.getOutputStream(), 65536)
            val items = sharedLinkItems

            if (path.startsWith("/download/")) {
                val idx = path.removePrefix("/download/").toIntOrNull() ?: 0
                val item = items.getOrNull(idx)
                if (item != null) {
                    val encodedName = URLEncoder.encode(item.name, "UTF-8").replace("+", "%20")
                    val header = buildString {
                        append("HTTP/1.1 200 OK\r\n")
                        append("Content-Type: " + item.mimeType + "\r\n")
                        if (item.sizeBytes > 0L) {
                            append("Content-Length: " + item.sizeBytes + "\r\n")
                        }
                        append("Content-Disposition: attachment; filename=\"" + encodedName + "\"\r\n")
                        append("Connection: close\r\n\r\n")
                    }
                    out.write(header.toByteArray(Charsets.UTF_8))
                    out.flush()

                    if (item.inlineTextBytes != null) {
                        out.write(item.inlineTextBytes)
                    } else if (item.directFile != null && item.directFile.exists()) {
                        FileInputStream(item.directFile).use { fis ->
                            val buf = ByteArray(262144)
                            var r: Int
                            while (fis.read(buf).also { r = it } != -1) {
                                out.write(buf, 0, r)
                            }
                        }
                    } else if (item.uri != null) {
                        context.contentResolver.openInputStream(item.uri)?.use { ins ->
                            val buf = ByteArray(262144)
                            var r: Int
                            while (ins.read(buf).also { r = it } != -1) {
                                out.write(buf, 0, r)
                            }
                        }
                    }
                    out.flush()
                    return
                }
            }

            val html = buildString {
                append("<!DOCTYPE html><html><head><meta charset='utf-8'>")
                append("<meta name='viewport' content='width=device-width,initial-scale=1'>")
                append("<title>Zeph Link Paylaşımı</title>")
                append("<style>body{background:#101818;color:#ECF4F6;font-family:sans-serif;padding:24px;}")
                append(".card{background:#182326;padding:16px;border-radius:14px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;}")
                append("a.btn{background:#6CD4FF;color:#002A3A;padding:10px 18px;border-radius:20px;text-decoration:none;font-weight:bold;}</style></head><body>")
                append("<h2>Zeph • Link Üzerinden Paylaşım</h2>")
                if (items.isEmpty()) {
                    append("<p>Henüz paylaşım için seçilmiş dosya yok.</p>")
                } else {
                    items.forEachIndexed { index, item ->
                        append("<div class='card'><div><strong>${item.name}</strong><br><small>${StorageHelper.formatFileSize(item.sizeBytes)}</small></div>")
                        append("<a class='btn' href='/download/$index'>İndir</a></div>")
                    }
                }
                append("</body></html>")
            }
            val bodyBytes = html.toByteArray(Charsets.UTF_8)
            val responseHeader = "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: " + bodyBytes.size + "\r\nConnection: close\r\n\r\n"
            out.write(responseHeader.toByteArray(Charsets.UTF_8))
            out.write(bodyBytes)
            out.flush()
        } catch (_: Throwable) {
        } finally {
            try {
                socket.close()
            } catch (_: Throwable) {
            }
        }
    }

    fun stop() {
        cancelAndPreservePartialFile()
        releaseInProcessHardwareLocks()
        activeTransferJob?.cancel()
        serverJob?.cancel()
        httpServerJob?.cancel()
    }
}
