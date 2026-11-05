package com.fasttransfer.p2p.transfer

import android.content.Context
import android.net.wifi.WifiManager
import android.os.Build
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
import com.fasttransfer.p2p.util.Mp4FastStartHelper
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
import java.io.Closeable
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileDescriptor
import java.io.FileInputStream
import java.io.RandomAccessFile
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.StandardSocketOptions
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.util.BitSet
import java.util.Collections
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

class ZeroCopyTransferEngine(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val getLocalDeviceModel: () -> String,
    private val onPrepareP2pGroup: () -> P2pGroupCredentials?,
    private val onJoinP2pGroup: (P2pGroupCredentials) -> Boolean,
    private val isHighBandUpgradeEnabled: () -> Boolean,
    private val resolveActiveBandLabel: (String) -> String = { it },
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onIncomingConsentRequest: (IncomingTransferRequest) -> Unit,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit,
    private val onTransferActiveStateChanged: (Boolean) -> Unit
) {
    companion object {
        const val SENDER_SOCKET_BUFFER_BYTES = 4 * 1024 * 1024 // 4 MB SO_SNDBUF per stream (Zero Bufferbloat)
        const val RECEIVER_SOCKET_BUFFER_BYTES = 16 * 1024 * 1024 // 16 MB SO_RCVBUF per stream
        const val DIRECT_BUFFER_BYTES = 4 * 1024 * 1024 // 4 MB Direct Buffer for small files
        const val STRIPE_BYTES = 1 * 1024 * 1024 // 1 MB Fine-Grained Stripe Unit (Zero HoL Stall)
        const val REORDER_RING_BUFFERS = 64 // 64 x 1 MB = 64 MB Off-Heap Reorder Pool
        const val PARALLEL_DATA_STREAMS = 6 // 6 Parallel TCP SocketChannels across 6 CPU Cores
        private const val PROTOCOL_MAGIC = 0x5A45503A // "ZEP:" Control & Handshake Channel
        private const val PROTOCOL_MAGIC_TURBO_DATA = 0x5A455054 // "ZEPT" Parallel Data Channel
        private const val DECISION_ACCEPTED: Int = 1
        private const val DECISION_REJECTED: Int = 2
    }

    private data class IndependentFileHandle(
        val fd: FileDescriptor,
        val channel: FileChannel,
        val closeables: List<Closeable>
    ) {
        fun closeAll() {
            for (c in closeables) {
                try {
                    c.close()
                } catch (_: Throwable) {
                }
            }
        }
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

    @Volatile
    private var activeReceiverChannel: SocketChannel? = null

    private val activeSenderWorkerChannels = CopyOnWriteArrayList<SocketChannel>()
    private val activeReceiverWorkerChannels = CopyOnWriteArrayList<SocketChannel>()

    @Volatile
    private var expectedSessionToken: Long = 0L

    private val incomingTurboPool = CopyOnWriteArrayList<SocketChannel>()
    private val incomingTurboLock = Object()

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

    fun cancelAndPreservePartialFile() {
        currentCancelFlag.set(true)
        isPaused.set(false)
        activeConsentLatch?.countDown()
        synchronized(incomingTurboLock) {
            incomingTurboLock.notifyAll()
        }

        val sChan = activeSenderChannel
        val rChan = activeReceiverChannel
        activeSenderChannel = null
        activeReceiverChannel = null

        val sWorkers = activeSenderWorkerChannels.toList()
        val rWorkers = activeReceiverWorkerChannels.toList()
        activeSenderWorkerChannels.clear()
        activeReceiverWorkerChannels.clear()

        scope.launch {
            sWorkers.forEach { abortChannelWithRst(it) }
            rWorkers.forEach { abortChannelWithRst(it) }
            abortChannelWithRst(sChan)
            abortChannelWithRst(rChan)
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

    private fun configureChannelSocketBuffers(channel: SocketChannel, isSender: Boolean) {
        val sndBuf = if (isSender) SENDER_SOCKET_BUFFER_BYTES else RECEIVER_SOCKET_BUFFER_BYTES
        val rcvBuf = RECEIVER_SOCKET_BUFFER_BYTES
        try {
            val socket = channel.socket()
            socket.tcpNoDelay = true
            socket.keepAlive = true
            socket.sendBufferSize = sndBuf
            socket.receiveBufferSize = rcvBuf
            // 0xA0 (DSCP CS5 -> UP=5 -> 802.11e WMM AC_VI High-Priority Video TXOP Burst Queue)
            socket.trafficClass = 0xA0
        } catch (_: Throwable) {
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            try {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true)
                channel.setOption(StandardSocketOptions.SO_SNDBUF, sndBuf)
                channel.setOption(StandardSocketOptions.SO_RCVBUF, rcvBuf)
                channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
            } catch (_: Throwable) {
            }
        }
    }

    private fun readFullyFromChannel(ch: SocketChannel, buf: ByteBuffer): Boolean {
        while (buf.hasRemaining()) {
            val r = ch.read(buf)
            if (r < 0) return false
        }
        return true
    }

    private fun writeFullyToChannel(ch: SocketChannel, buf: ByteBuffer): Boolean {
        while (buf.hasRemaining()) {
            val w = ch.write(buf)
            if (w < 0) return false
        }
        return true
    }

    private fun openIndependentFileHandle(item: TransferItem): IndependentFileHandle? {
        try {
            if (item.directFile != null && item.directFile.exists()) {
                val fis = FileInputStream(item.directFile)
                return IndependentFileHandle(
                    fd = fis.fd,
                    channel = fis.channel,
                    closeables = listOf(fis.channel, fis)
                )
            }
            if (item.uri != null) {
                val pfd = context.contentResolver.openFileDescriptor(item.uri, "r") ?: return null
                val fis = FileInputStream(pfd.fileDescriptor)
                return IndependentFileHandle(
                    fd = pfd.fileDescriptor,
                    channel = fis.channel,
                    closeables = listOf(fis.channel, fis, pfd)
                )
            }
        } catch (_: Throwable) {
        }
        return null
    }

    private fun writeBufferAtOffsetSingleThread(
        fd: FileDescriptor,
        fc: FileChannel,
        buf: ByteBuffer,
        offset: Long
    ): Long {
        var writtenTotal = 0L
        while (buf.hasRemaining()) {
            val w = try {
                Os.pwrite(fd, buf, offset + writtenTotal)
            } catch (_: Throwable) {
                fc.write(buf, offset + writtenTotal)
            }
            if (w <= 0) break
            writtenTotal += w.toLong()
        }
        return writtenTotal
    }

    private fun readBufferAtOffsetLockless(
        fd: FileDescriptor,
        fc: FileChannel,
        buf: ByteBuffer,
        offset: Long
    ): Int {
        var readTotal = 0
        while (buf.hasRemaining()) {
            val r = try {
                Os.pread(fd, buf, offset + readTotal)
            } catch (_: Throwable) {
                fc.read(buf, offset + readTotal)
            }
            if (r <= 0) break
            readTotal += r
        }
        return readTotal
    }

    private fun computeSustainedSpeedMbps(
        samples: ConcurrentLinkedDeque<Pair<Long, Long>>,
        nowNs: Long,
        totalBytes: Long,
        startTimeNs: Long
    ): Double {
        samples.addLast(nowNs to totalBytes)
        val windowNs = 2_500_000_000L
        while (samples.size > 2) {
            val first = samples.peekFirst() ?: break
            if (nowNs - first.first > windowNs) {
                samples.pollFirst()
            } else {
                break
            }
        }
        val totalElapsedSec = ((nowNs - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.05)
        val cumulativeMbps = (totalBytes / (1024.0 * 1024.0)) / totalElapsedSec

        val oldest = samples.peekFirst()
        if (oldest != null && nowNs - oldest.first >= 400_000_000L) {
            val windowSec = (nowNs - oldest.first) / 1_000_000_000.0
            val windowBytes = (totalBytes - oldest.second).coerceAtLeast(0L)
            val windowMbps = (windowBytes / (1024.0 * 1024.0)) / windowSec
            return (windowMbps * 0.65) + (cumulativeMbps * 0.35)
        }
        return cumulativeMbps
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
                    serverChannel.socket().receiveBufferSize = RECEIVER_SOCKET_BUFFER_BYTES
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
            configureChannelSocketBuffers(clientChannel, isSender = false)

            val dis = DataInputStream(clientChannel.socket().getInputStream())
            val magic = dis.readInt()
            if (magic == PROTOCOL_MAGIC_TURBO_DATA) {
                val token = dis.readLong()
                synchronized(incomingTurboLock) {
                    if (token != 0L && token == expectedSessionToken) {
                        incomingTurboPool.add(clientChannel)
                        incomingTurboLock.notifyAll()
                    } else {
                        closeChannelGracefully(clientChannel)
                    }
                }
                return
            }
            if (magic != PROTOCOL_MAGIC) {
                closeChannelGracefully(clientChannel)
                return
            }
            handleIncomingClientChannel(clientChannel, dis)
        } catch (_: Throwable) {
            closeChannelGracefully(clientChannel)
        }
    }

    private fun handleIncomingClientChannel(clientChannel: SocketChannel, dis: DataInputStream) {
        activeReceiverWorkerChannels.forEach { abortChannelWithRst(it) }
        activeReceiverWorkerChannels.clear()
        abortChannelWithRst(activeReceiverChannel)

        val sessionCancel = AtomicBoolean(false)
        currentCancelFlag = sessionCancel
        activeReceiverChannel = clientChannel
        isPaused.set(false)

        try {
            val socket = clientChannel.socket()
            val senderIp = socket.inetAddress?.hostAddress ?: "Bilinmeyen IP"
            val dos = DataOutputStream(socket.getOutputStream())

            // 1. Read Handshake Metadata + Sender GO Capability Score
            val senderAlias = dis.readUTF()
            val senderModel = dis.readUTF().ifBlank { "Android" }
            var transportLabel = dis.readUTF().ifBlank { "Doğrudan Köprü (6GHz / 5GHz)" }
            val senderGoScore = dis.readInt()
            val fileCount = dis.readInt()
            val totalBatchBytes = dis.readLong()
            val firstFileName = dis.readUTF()
            val firstFileSize = dis.readLong()
            val previewCount = dis.readInt()
            val previewLines = mutableListOf<String>()
            for (i in 0 until previewCount) {
                previewLines.add(dis.readUTF())
            }

            // 2. Smart Capability-Based GO Election
            val isAlreadyDirectLink = CompatibilityHelper.isAlreadyDirectOrHotspotConnection(context, senderIp)
            if (isAlreadyDirectLink) {
                transportLabel = resolveActiveBandLabel(transportLabel)
            }
            val receiverGoScore = CompatibilityHelper.getDeviceGoCapabilityScore(context)
            val preferReceiverAsGo = receiverGoScore >= senderGoScore

            val p2pGroupDeferred = CompletableDeferred<P2pGroupCredentials?>()
            if (!isAlreadyDirectLink && isHighBandUpgradeEnabled() && preferReceiverAsGo) {
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

            val sessionToken = Random.nextLong(1L, Long.MAX_VALUE)
            synchronized(incomingTurboLock) {
                incomingTurboPool.forEach { closeChannelGracefully(it) }
                incomingTurboPool.clear()
                expectedSessionToken = sessionToken
            }

            dos.writeByte(DECISION_ACCEPTED)
            dos.writeLong(sessionToken)

            val receiverP2pCreds: P2pGroupCredentials? = runBlocking {
                withTimeoutOrNull(3000L) {
                    p2pGroupDeferred.await()
                }
            }

            // Phase A: Offer Receiver-GO if Receiver formed a valid 5 GHz group (<= 32 byte SSID)
            var upgradedToP2p = false
            if (receiverP2pCreds != null &&
                !isAlreadyDirectLink &&
                receiverP2pCreds.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
                receiverP2pCreds.freqMhz >= 5000
            ) {
                dos.writeBoolean(true)
                dos.writeUTF(receiverP2pCreds.ssid)
                dos.writeUTF(receiverP2pCreds.passphrase)
                dos.writeUTF(receiverP2pCreds.ownerIp)
                dos.writeInt(receiverP2pCreds.freqMhz)
                dos.writeUTF(receiverP2pCreds.bandLabel)
                dos.flush()

                val senderJoinedReceiverGo = try {
                    socket.soTimeout = 5000
                    dis.readBoolean()
                } catch (_: Throwable) {
                    false
                } finally {
                    try { socket.soTimeout = 0 } catch (_: Throwable) {}
                }
                if (senderJoinedReceiverGo) {
                    upgradedToP2p = true
                    transportLabel = receiverP2pCreds.bandLabel
                }
            } else {
                dos.writeBoolean(false)
                dos.flush()
            }

            // Phase B: Bidirectional Role-Reversal!
            val allowSenderGoFallback = !isAlreadyDirectLink && !upgradedToP2p && isHighBandUpgradeEnabled()
            dos.writeBoolean(allowSenderGoFallback)
            dos.flush()

            if (allowSenderGoFallback) {
                val senderCreatedGo = try {
                    socket.soTimeout = 5500
                    dis.readBoolean()
                } catch (_: Throwable) {
                    false
                } finally {
                    try { socket.soTimeout = 0 } catch (_: Throwable) {}
                }

                if (senderCreatedGo) {
                    val sSsid = dis.readUTF()
                    val sPwd = dis.readUTF()
                    val sOwnerIp = dis.readUTF()
                    val sFreq = dis.readInt()
                    val sLabel = dis.readUTF()

                    val senderCreds = P2pGroupCredentials(
                        ssid = sSsid,
                        passphrase = sPwd,
                        ownerIp = sOwnerIp,
                        freqMhz = sFreq,
                        bandLabel = sLabel
                    )
                    val joinedSenderGo = onJoinP2pGroup(senderCreds)
                    var myP2pClientIp: String? = null
                    if (joinedSenderGo) {
                        for (waitIdx in 0 until 22) {
                            val ip = CompatibilityHelper.getLocalP2pInterfaceIp()
                            if (!ip.isNullOrBlank() && ip != "192.168.49.1") {
                                myP2pClientIp = ip
                                break
                            }
                            Thread.sleep(75L)
                        }
                    }

                    if (!myP2pClientIp.isNullOrBlank()) {
                        upgradedToP2p = true
                        transportLabel = sLabel
                        dos.writeBoolean(true)
                        dos.writeUTF(myP2pClientIp)
                        dos.flush()
                    } else {
                        dos.writeBoolean(false)
                        dos.flush()
                    }
                }
            }

            if (!upgradedToP2p) {
                transportLabel = resolveActiveBandLabel(transportLabel)
            }

            // 4. Negotiate 6 Parallel Turbo Data Channels
            val senderOpenedStreams = try {
                socket.soTimeout = 5000
                dis.readInt()
            } catch (_: Throwable) {
                0
            } finally {
                try { socket.soTimeout = 0 } catch (_: Throwable) {}
            }

            if (senderOpenedStreams > 0) {
                val deadline = System.currentTimeMillis() + 3200L
                synchronized(incomingTurboLock) {
                    while (incomingTurboPool.size < senderOpenedStreams &&
                        System.currentTimeMillis() < deadline &&
                        !sessionCancel.get()
                    ) {
                        val rem = (deadline - System.currentTimeMillis()).coerceAtLeast(1L)
                        incomingTurboLock.wait(rem.coerceAtMost(120L))
                    }
                }
            }

            val workerChannels = synchronized(incomingTurboLock) {
                val list = incomingTurboPool.toList()
                incomingTurboPool.clear()
                expectedSessionToken = 0L
                list
            }
            activeReceiverWorkerChannels.addAll(workerChannels)

            dos.writeInt(workerChannels.size)
            dos.flush()

            val singleDirectBuf = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            // 5. Receive Each File
            for (fileIdx in 0 until fileCount) {
                if (sessionCancel.get()) break

                val fileName = dis.readUTF()
                val expectedFileSize = dis.readLong()
                val useParallelStripes = dis.readBoolean()
                val hasMp4Meta = dis.readBoolean()

                val mp4Meta: Mp4FastStartHelper.Mp4ContainerMetadata? = if (hasMp4Meta) {
                    val mdatOff = dis.readLong()
                    val mdatHdrSize = dis.readInt()
                    val origHdr = ByteArray(mdatHdrSize)
                    dis.readFully(origHdr)
                    val moovLen = dis.readInt()
                    val moovBytes = if (moovLen > 0) {
                        ByteArray(moovLen).also { dis.readFully(it) }
                    } else {
                        ByteArray(0)
                    }
                    Mp4FastStartHelper.Mp4ContainerMetadata(
                        mdatOffset = mdatOff,
                        mdatHeaderSize = mdatHdrSize,
                        originalMdatHeader = origHdr,
                        tailMoovBytes = moovBytes
                    )
                } else {
                    null
                }

                val targetFile: File = StorageHelper.createDirectDownloadTarget(context, fileName)
                val totalBytesWrittenAtomic = AtomicLong(0L)
                val contiguousWatermarkBytes = AtomicLong(0L)
                val startTimeNs = System.nanoTime()
                val startWallMs = System.currentTimeMillis()
                var lastUiUpdateMs = 0L
                val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()

                var raf: RandomAccessFile? = null
                var fileChannel: FileChannel? = null

                try {
                    raf = RandomAccessFile(targetFile, "rw")
                    val rawFd = raf.fd
                    fileChannel = raf.channel
                    fileChannel.position(0L)

                    if (useParallelStripes && workerChannels.isNotEmpty() && expectedFileSize > 0L) {
                        val totalStripes = ((expectedFileSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
                        val freeBuffers = ArrayBlockingQueue<ByteBuffer>(REORDER_RING_BUFFERS)
                        for (i in 0 until REORDER_RING_BUFFERS) {
                            freeBuffers.put(ByteBuffer.allocateDirect(STRIPE_BYTES))
                        }

                        val completedStripes = ConcurrentSkipListMap<Int, ByteBuffer>()
                        val writtenStripesBitSet = BitSet(totalStripes + 8)
                        var nextContiguousStripe = 0
                        val activeReaders = AtomicInteger(workerChannels.size)
                        val writerError = AtomicBoolean(false)
                        val reorderLock = Object()
                        val fc = fileChannel

                        // Spawn 6 Parallel Network Reader Threads across 6 CPU Cores
                        val readerThreads = workerChannels.mapIndexed { chIdx, wChan ->
                            Thread {
                                try {
                                    Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                                    val hdrBuf = ByteBuffer.allocateDirect(8).order(ByteOrder.BIG_ENDIAN)
                                    while (!sessionCancel.get() && !writerError.get()) {
                                        hdrBuf.clear()
                                        if (!readFullyFromChannel(wChan, hdrBuf)) break
                                        hdrBuf.flip()
                                        val stripeIdx = hdrBuf.int
                                        val stripeLen = hdrBuf.int
                                        if (stripeIdx < 0 || stripeLen <= 0 || stripeLen > STRIPE_BYTES) {
                                            break
                                        }

                                        var dataBuf: ByteBuffer? = null
                                        while (dataBuf == null && !sessionCancel.get() && !writerError.get()) {
                                            dataBuf = freeBuffers.poll(80L, TimeUnit.MILLISECONDS)
                                        }
                                        if (dataBuf == null) break

                                        dataBuf.clear()
                                        dataBuf.limit(stripeLen)

                                        var eofReached = false
                                        while (dataBuf.hasRemaining() && !sessionCancel.get()) {
                                            val r = wChan.read(dataBuf)
                                            if (r < 0) {
                                                eofReached = true
                                                break
                                            }
                                        }

                                        dataBuf.flip()
                                        if (dataBuf.remaining() == stripeLen) {
                                            completedStripes[stripeIdx] = dataBuf
                                            synchronized(reorderLock) {
                                                reorderLock.notifyAll()
                                            }
                                        } else {
                                            dataBuf.clear()
                                            freeBuffers.offer(dataBuf)
                                        }

                                        if (eofReached) break
                                    }
                                } catch (_: Throwable) {
                                } finally {
                                    if (activeReaders.decrementAndGet() == 0) {
                                        synchronized(reorderLock) {
                                            reorderLock.notifyAll()
                                        }
                                    }
                                }
                            }.apply {
                                name = "Zeph-NetReader-$chIdx"
                                start()
                            }
                        }

                        // Dedicated Single UFS Writer Thread (Zero Multi-Thread FUSE Contention + Zero HoL Stall!)
                        var pauseTailApplied = false
                        while (nextContiguousStripe < totalStripes && !sessionCancel.get() && !writerError.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                if (!pauseTailApplied) {
                                    pauseTailApplied = true
                                    try {
                                        // Allow up to 180ms for in-flight stripes on the 6 sockets to land so
                                        // nextContiguousStripe catches up completely before pause commit
                                        val drainDeadline = System.currentTimeMillis() + 180L
                                        while (System.currentTimeMillis() < drainDeadline) {
                                            val entry = completedStripes.pollFirstEntry()
                                            if (entry != null) {
                                                val sIdx = entry.key
                                                val buf = entry.value
                                                val offset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                                val w = writeBufferAtOffsetSingleThread(rawFd, fc, buf, offset)
                                                totalBytesWrittenAtomic.addAndGet(w)
                                                buf.clear()
                                                freeBuffers.offer(buf)
                                                writtenStripesBitSet.set(sIdx)
                                                if (sIdx == nextContiguousStripe) {
                                                    nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                                                }
                                            } else {
                                                Thread.sleep(15L)
                                            }
                                        }
                                        val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                            .coerceAtMost(expectedFileSize)
                                        contiguousWatermarkBytes.set(contiguousNow)
                                        if (mp4Meta != null && contiguousNow > 0L && contiguousNow < expectedFileSize) {
                                            Mp4FastStartHelper.applyPartialMoovTail(fc, contiguousNow, mp4Meta)
                                        } else {
                                            fc.force(true)
                                        }
                                    } catch (_: Throwable) {
                                    }
                                }
                                onProgressUpdate(
                                    TransferProgress(
                                        fileName = targetFile.name,
                                        fileIndex = fileIdx,
                                        totalFiles = fileCount,
                                        bytesTransferred = contiguousWatermarkBytes.get(),
                                        totalBytes = expectedFileSize,
                                        speedMbps = 0.0,
                                        etaSeconds = 0L,
                                        elapsedMillis = System.currentTimeMillis() - startWallMs,
                                        status = TransferStatus.PAUSED,
                                        transportLabel = transportLabel,
                                        isSending = false,
                                        savedFilePath = targetFile.absolutePath,
                                        message = "Transfer Duraklatıldı (Oynatmaya Hazır)"
                                    )
                                )
                                Thread.sleep(90L)
                            }
                            if (pauseTailApplied && !sessionCancel.get()) {
                                pauseTailApplied = false
                                speedSamples.clear()
                                val contiguousNow = contiguousWatermarkBytes.get()
                                if (writtenStripesBitSet.length() > nextContiguousStripe) {
                                    writtenStripesBitSet.clear(nextContiguousStripe, writtenStripesBitSet.length())
                                }
                                totalBytesWrittenAtomic.set(contiguousNow)
                                if (mp4Meta != null) {
                                    Mp4FastStartHelper.restoreOriginalMdatForResume(
                                        fc,
                                        contiguousNow,
                                        mp4Meta
                                    )
                                }
                            }
                            if (sessionCancel.get()) break

                            // Priority 1: Write nextContiguousStripe immediately if available
                            var targetStripeIdx = nextContiguousStripe
                            var readyBuf = completedStripes.remove(targetStripeIdx)

                            // Priority 2 (Anti-HoL-Stall): If nextContiguousStripe is still in flight on one socket
                            // while other sockets have delivered >= 4 stripes (or readers finished), write the
                            // lowest available stripe immediately so freeBuffers NEVER runs dry and sockets NEVER stall!
                            if (readyBuf == null && (completedStripes.size >= 4 || activeReaders.get() == 0)) {
                                val firstEntry = completedStripes.pollFirstEntry()
                                if (firstEntry != null) {
                                    targetStripeIdx = firstEntry.key
                                    readyBuf = firstEntry.value
                                }
                            }

                            if (readyBuf == null) {
                                if (activeReaders.get() == 0 && completedStripes.isEmpty()) {
                                    break
                                }
                                synchronized(reorderLock) {
                                    if (completedStripes.isEmpty() && activeReaders.get() > 0 && !sessionCancel.get()) {
                                        reorderLock.wait(12L)
                                    }
                                }
                                continue
                            }

                            val stripeOffset = targetStripeIdx.toLong() * STRIPE_BYTES.toLong()
                            val written = writeBufferAtOffsetSingleThread(rawFd, fc, readyBuf, stripeOffset)
                            totalBytesWrittenAtomic.addAndGet(written)
                            readyBuf.clear()
                            freeBuffers.offer(readyBuf)

                            writtenStripesBitSet.set(targetStripeIdx)
                            if (targetStripeIdx == nextContiguousStripe) {
                                nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                            }
                            val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                .coerceAtMost(expectedFileSize)
                            contiguousWatermarkBytes.set(contiguousNow)

                            val doneBytes = totalBytesWrittenAtomic.get().coerceAtMost(expectedFileSize)
                            val nowMs = System.currentTimeMillis()
                            if (nowMs - lastUiUpdateMs >= 120L || doneBytes == expectedFileSize) {
                                lastUiUpdateMs = nowMs
                                val nowNs = System.nanoTime()
                                val speedMbps = computeSustainedSpeedMbps(
                                    samples = speedSamples,
                                    nowNs = nowNs,
                                    totalBytes = doneBytes,
                                    startTimeNs = startTimeNs
                                )
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

                        // Drain any remaining contiguous stripes already in memory upon completion or cancel
                        while (nextContiguousStripe < totalStripes) {
                            val pending = completedStripes.remove(nextContiguousStripe) ?: break
                            val stripeOffset = nextContiguousStripe.toLong() * STRIPE_BYTES.toLong()
                            val written = writeBufferAtOffsetSingleThread(rawFd, fc, pending, stripeOffset)
                            totalBytesWrittenAtomic.addAndGet(written)
                            pending.clear()
                            writtenStripesBitSet.set(nextContiguousStripe)
                            nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                        }
                        val finalContiguous = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                            .coerceAtMost(expectedFileSize)
                        contiguousWatermarkBytes.set(finalContiguous)

                        for (rt in readerThreads) {
                            try {
                                rt.join(400L)
                            } catch (_: Throwable) {
                            }
                        }
                    } else {
                        val streamChan = workerChannels.firstOrNull() ?: clientChannel
                        while (contiguousWatermarkBytes.get() < expectedFileSize && !sessionCancel.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                try {
                                    val contiguousNow = contiguousWatermarkBytes.get()
                                    if (mp4Meta != null && contiguousNow > 0L && contiguousNow < expectedFileSize) {
                                        Mp4FastStartHelper.applyPartialMoovTail(fileChannel, contiguousNow, mp4Meta)
                                    } else {
                                        fileChannel.force(true)
                                    }
                                } catch (_: Throwable) {
                                }
                                Thread.sleep(90L)
                            }
                            if (sessionCancel.get()) break

                            singleDirectBuf.clear()
                            val remaining = expectedFileSize - contiguousWatermarkBytes.get()
                            if (remaining < singleDirectBuf.capacity()) {
                                singleDirectBuf.limit(remaining.toInt())
                            }

                            val bytesRead = streamChan.read(singleDirectBuf)
                            if (bytesRead < 0) break
                            if (bytesRead == 0) continue

                            singleDirectBuf.flip()
                            val offset = contiguousWatermarkBytes.get()
                            val w = writeBufferAtOffsetSingleThread(rawFd, fileChannel, singleDirectBuf, offset)
                            contiguousWatermarkBytes.addAndGet(w)
                            totalBytesWrittenAtomic.set(contiguousWatermarkBytes.get())

                            val doneBytes = contiguousWatermarkBytes.get()
                            val nowMs = System.currentTimeMillis()
                            if (nowMs - lastUiUpdateMs >= 120L || doneBytes == expectedFileSize) {
                                lastUiUpdateMs = nowMs
                                val nowNs = System.nanoTime()
                                val speedMbps = computeSustainedSpeedMbps(
                                    samples = speedSamples,
                                    nowNs = nowNs,
                                    totalBytes = doneBytes,
                                    startTimeNs = startTimeNs
                                )
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
                    val contiguousBytes = contiguousWatermarkBytes.get().coerceAtMost(expectedFileSize)
                    val totalBytesReceived = if (totalBytesWrittenAtomic.get() >= expectedFileSize) {
                        expectedFileSize
                    } else {
                        contiguousBytes
                    }
                    val isPartial = totalBytesReceived in 1L until expectedFileSize
                    try {
                        if (fileChannel != null && fileChannel.isOpen) {
                            if (isPartial && mp4Meta != null) {
                                Mp4FastStartHelper.applyPartialMoovTail(fileChannel, totalBytesReceived, mp4Meta)
                            } else {
                                fileChannel.truncate(totalBytesReceived)
                                fileChannel.force(true)
                            }
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
            val workers = activeReceiverWorkerChannels.toList()
            activeReceiverWorkerChannels.clear()
            if (sessionCancel.get()) {
                workers.forEach { abortChannelWithRst(it) }
                abortChannelWithRst(clientChannel)
            } else {
                workers.forEach { closeChannelGracefully(it) }
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

        activeSenderWorkerChannels.forEach { abortChannelWithRst(it) }
        activeSenderWorkerChannels.clear()
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
        var transportLabel = resolveActiveBandLabel(initialTransportLabel)
        var activeDataTargetIp = hostIp

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
            configureChannelSocketBuffers(controlChannel, isSender = true)
            CompatibilityHelper.bindSocketToOptimalInterface(context, controlChannel.socket(), hostIp)
            controlChannel.socket().connect(InetSocketAddress(hostIp, port), 5000)

            val dos = DataOutputStream(controlChannel.socket().getOutputStream())
            val dis = DataInputStream(controlChannel.socket().getInputStream())

            // 1. Send Handshake Metadata + Local GO Capability Score
            val localGoScore = CompatibilityHelper.getDeviceGoCapabilityScore(context)
            dos.writeInt(PROTOCOL_MAGIC)
            dos.writeUTF(getLocalAlias())
            dos.writeUTF(getLocalDeviceModel())
            dos.writeUTF(transportLabel)
            dos.writeInt(localGoScore)
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

            val sessionToken = dis.readLong()

            // 2. Phase A: Check if Receiver offered an Automatic 5 GHz Direct P2P Bridge (Receiver-GO)
            val hasReceiverP2p = dis.readBoolean()
            if (hasReceiverP2p && !sessionCancel.get()) {
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
                    activeDataTargetIp = p2pOwnerIp
                    transportLabel = p2pLabel
                    dos.writeBoolean(true)
                    dos.flush()
                } else {
                    dos.writeBoolean(false)
                    dos.flush()
                }
            }

            // 3. Phase B: Bidirectional Role-Reversal!
            val receiverRequestedSenderGo = dis.readBoolean()
            if (receiverRequestedSenderGo && !sessionCancel.get()) {
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
                        transportLabel = "5 GHz Doğrudan Köprü",
                        isSending = true,
                        message = "5 GHz Yüksek Hızlı Doğrudan Köprü kuruluyor..."
                    )
                )
                val myGoCreds = onPrepareP2pGroup()
                if (myGoCreds != null &&
                    myGoCreds.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
                    !sessionCancel.get()
                ) {
                    dos.writeBoolean(true)
                    dos.writeUTF(myGoCreds.ssid)
                    dos.writeUTF(myGoCreds.passphrase)
                    dos.writeUTF(myGoCreds.ownerIp)
                    dos.writeInt(myGoCreds.freqMhz)
                    dos.writeUTF(myGoCreds.bandLabel)
                    dos.flush()

                    val receiverJoinedMyGo = dis.readBoolean()
                    if (receiverJoinedMyGo) {
                        val receiverP2pIp = dis.readUTF()
                        if (receiverP2pIp.isNotBlank()) {
                            activeDataTargetIp = receiverP2pIp
                            transportLabel = myGoCreds.bandLabel
                        }
                    }
                } else {
                    dos.writeBoolean(false)
                    dos.flush()
                }
            }

            transportLabel = resolveActiveBandLabel(transportLabel)

            // 4. Open 6 Parallel Turbo Data SocketChannels to activeDataTargetIp:port
            for (streamIdx in 0 until PARALLEL_DATA_STREAMS) {
                if (sessionCancel.get()) break
                var openedChan: SocketChannel? = null
                for (attempt in 0 until 8) {
                    if (sessionCancel.get()) break
                    try {
                        val candidate = SocketChannel.open()
                        candidate.configureBlocking(true)
                        configureChannelSocketBuffers(candidate, isSender = true)
                        CompatibilityHelper.bindSocketToOptimalInterface(
                            context,
                            candidate.socket(),
                            activeDataTargetIp
                        )
                        candidate.socket().connect(InetSocketAddress(activeDataTargetIp, port), 1000)
                        val turboOut = DataOutputStream(candidate.socket().getOutputStream())
                        turboOut.writeInt(PROTOCOL_MAGIC_TURBO_DATA)
                        turboOut.writeLong(sessionToken)
                        turboOut.flush()
                        openedChan = candidate
                        break
                    } catch (_: Throwable) {
                        Thread.sleep(90L)
                    }
                }
                if (openedChan != null) {
                    workerChannels.add(openedChan)
                }
            }

            if (workerChannels.isEmpty() && activeDataTargetIp != hostIp && !sessionCancel.get()) {
                activeDataTargetIp = hostIp
                transportLabel = resolveActiveBandLabel(initialTransportLabel)
                for (streamIdx in 0 until PARALLEL_DATA_STREAMS) {
                    if (sessionCancel.get()) break
                    try {
                        val candidate = SocketChannel.open()
                        candidate.configureBlocking(true)
                        configureChannelSocketBuffers(candidate, isSender = true)
                        CompatibilityHelper.bindSocketToOptimalInterface(context, candidate.socket(), hostIp)
                        candidate.socket().connect(InetSocketAddress(hostIp, port), 1000)
                        val turboOut = DataOutputStream(candidate.socket().getOutputStream())
                        turboOut.writeInt(PROTOCOL_MAGIC_TURBO_DATA)
                        turboOut.writeLong(sessionToken)
                        turboOut.flush()
                        workerChannels.add(candidate)
                    } catch (_: Throwable) {
                    }
                }
            }

            activeSenderWorkerChannels.addAll(workerChannels)
            dos.writeInt(workerChannels.size)
            dos.flush()

            val confirmedWorkerCount = dis.readInt()
            val canUseParallelWorkers = confirmedWorkerCount > 0 && workerChannels.isNotEmpty()

            val fallbackDirectBuffer = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            // 5. Send Each File
            for ((fileIdx, item) in items.withIndex()) {
                if (sessionCancel.get()) break

                var primaryHandle: IndependentFileHandle? = null

                try {
                    val actualSize: Long
                    if (item.inlineTextBytes != null) {
                        actualSize = item.inlineTextBytes.size.toLong()
                    } else {
                        primaryHandle = openIndependentFileHandle(item)
                        if (primaryHandle != null) {
                            actualSize = if (item.sizeBytes > 0L) item.sizeBytes else primaryHandle.channel.size()
                        } else {
                            continue
                        }
                    }

                    val useParallelStripes = canUseParallelWorkers &&
                        primaryHandle != null &&
                        item.inlineTextBytes == null &&
                        actualSize >= 4L * 1024L * 1024L

                    val mp4Meta = if (primaryHandle != null && Mp4FastStartHelper.isMp4FamilyFile(item.name)) {
                        Mp4FastStartHelper.extractMp4Metadata(primaryHandle.channel, actualSize)
                    } else {
                        null
                    }

                    dos.writeUTF(item.name)
                    dos.writeLong(actualSize)
                    dos.writeBoolean(useParallelStripes)
                    if (mp4Meta != null) {
                        dos.writeBoolean(true)
                        dos.writeLong(mp4Meta.mdatOffset)
                        dos.writeInt(mp4Meta.mdatHeaderSize)
                        dos.write(mp4Meta.originalMdatHeader)
                        dos.writeInt(mp4Meta.tailMoovBytes.size)
                        if (mp4Meta.tailMoovBytes.isNotEmpty()) {
                            dos.write(mp4Meta.tailMoovBytes)
                        }
                    } else {
                        dos.writeBoolean(false)
                    }
                    dos.flush()

                    val totalBytesSentAtomic = AtomicLong(0L)
                    val startTimeNs = System.nanoTime()
                    val startWallMs = System.currentTimeMillis()
                    val lastUiUpdateMs = AtomicLong(0L)
                    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()

                    if (useParallelStripes && primaryHandle != null) {
                        val totalStripes = ((actualSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
                        val nextStripeIdx = AtomicInteger(0)
                        val activeChannels = workerChannels.take(confirmedWorkerCount)

                        val workerHandles = activeChannels.mapIndexed { idx, _ ->
                            if (idx == 0) primaryHandle else (openIndependentFileHandle(item) ?: primaryHandle)
                        }

                        try {
                            val senderWorkers = activeChannels.mapIndexed { wIdx, wChan ->
                                val wHandle = workerHandles[wIdx]
                                Thread {
                                    try {
                                        Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                                        val hdrBuf = ByteBuffer.allocateDirect(8).order(ByteOrder.BIG_ENDIAN)
                                        val workerDirectBuf = ByteBuffer.allocateDirect(STRIPE_BYTES)

                                        while (!sessionCancel.get()) {
                                            while (isPaused.get() && !sessionCancel.get()) {
                                                if (wIdx == 0) {
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
                                                }
                                                Thread.sleep(90L)
                                            }
                                            if (sessionCancel.get()) break

                                            val sIdx = nextStripeIdx.getAndIncrement()
                                            if (sIdx >= totalStripes) {
                                                hdrBuf.clear()
                                                hdrBuf.putInt(-1)
                                                hdrBuf.putInt(0)
                                                hdrBuf.flip()
                                                writeFullyToChannel(wChan, hdrBuf)
                                                break
                                            }

                                            val stripeOffset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                            val stripeLen = (actualSize - stripeOffset)
                                                .coerceAtMost(STRIPE_BYTES.toLong()).toInt()

                                            hdrBuf.clear()
                                            hdrBuf.putInt(sIdx)
                                            hdrBuf.putInt(stripeLen)
                                            hdrBuf.flip()
                                            if (!writeFullyToChannel(wChan, hdrBuf)) break

                                            workerDirectBuf.clear()
                                            workerDirectBuf.limit(stripeLen)
                                            val readBytes = readBufferAtOffsetLockless(
                                                fd = wHandle.fd,
                                                fc = wHandle.channel,
                                                buf = workerDirectBuf,
                                                offset = stripeOffset
                                            )
                                            if (readBytes < stripeLen) break
                                            workerDirectBuf.flip()

                                            while (workerDirectBuf.hasRemaining() && !sessionCancel.get()) {
                                                val w = wChan.write(workerDirectBuf)
                                                if (w < 0) break
                                                if (w > 0) {
                                                    totalBytesSentAtomic.addAndGet(w.toLong())
                                                }
                                            }

                                            val nowMs = System.currentTimeMillis()
                                            val prevUi = lastUiUpdateMs.get()
                                            if (nowMs - prevUi >= 120L && lastUiUpdateMs.compareAndSet(prevUi, nowMs)) {
                                                val curSent = totalBytesSentAtomic.get().coerceAtMost(actualSize)
                                                val nowNs = System.nanoTime()
                                                val speedMbps = computeSustainedSpeedMbps(
                                                    samples = speedSamples,
                                                    nowNs = nowNs,
                                                    totalBytes = curSent,
                                                    startTimeNs = startTimeNs
                                                )
                                                val remBytes = (actualSize - curSent).coerceAtLeast(0L)
                                                val etaSec = if (speedMbps > 0.01) {
                                                    ((remBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                                                } else {
                                                    0L
                                                }
                                                val reportedBytes = if (curSent >= actualSize) (actualSize - 1L).coerceAtLeast(0L) else curSent
                                                onProgressUpdate(
                                                    TransferProgress(
                                                        fileName = item.name,
                                                        fileIndex = fileIdx,
                                                        totalFiles = items.size,
                                                        bytesTransferred = reportedBytes,
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
                                    } catch (_: Throwable) {
                                    }
                                }.apply {
                                    name = "Zeph-NetSender-$wIdx"
                                    start()
                                }
                            }

                            for (sw in senderWorkers) {
                                try {
                                    sw.join()
                                } catch (_: Throwable) {
                                }
                            }
                        } finally {
                            for (idx in 1 until workerHandles.size) {
                                val h = workerHandles[idx]
                                if (h !== primaryHandle) {
                                    h.closeAll()
                                }
                            }
                        }
                    } else {
                        val streamChan = workerChannels.firstOrNull() ?: controlChannel
                        if (item.inlineTextBytes != null) {
                            val buf = ByteBuffer.wrap(item.inlineTextBytes)
                            while (buf.hasRemaining() && !sessionCancel.get()) {
                                totalBytesSentAtomic.addAndGet(streamChan.write(buf).toLong())
                            }
                        } else if (primaryHandle != null) {
                            while (totalBytesSentAtomic.get() < actualSize && !sessionCancel.get()) {
                                while (isPaused.get() && !sessionCancel.get()) {
                                    Thread.sleep(90L)
                                }
                                if (sessionCancel.get()) break

                                fallbackDirectBuffer.clear()
                                val curSent = totalBytesSentAtomic.get()
                                val rem = actualSize - curSent
                                if (rem < fallbackDirectBuffer.capacity()) {
                                    fallbackDirectBuffer.limit(rem.toInt())
                                }
                                val readCount = readBufferAtOffsetLockless(
                                    fd = primaryHandle.fd,
                                    fc = primaryHandle.channel,
                                    buf = fallbackDirectBuffer,
                                    offset = curSent
                                )
                                if (readCount <= 0) break
                                fallbackDirectBuffer.flip()
                                while (fallbackDirectBuffer.hasRemaining() && !sessionCancel.get()) {
                                    val written = streamChan.write(fallbackDirectBuffer)
                                    if (written <= 0) break
                                    totalBytesSentAtomic.addAndGet(written.toLong())
                                }
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
                    primaryHandle?.closeAll()
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
            activeSenderWorkerChannels.clear()
            if (sessionCancel.get()) {
                workerChannels.forEach { abortChannelWithRst(it) }
                abortChannelWithRst(controlChannel)
            } else {
                workerChannels.forEach { closeChannelGracefully(it) }
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
            socket.sendBufferSize = RECEIVER_SOCKET_BUFFER_BYTES
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
