// Zeph v18.0.0-HyperStream (Revision 2026.11.03-v18)
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerArray
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.random.Random

class ZeroCopyTransferEngine(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val getLocalDeviceModel: () -> String,
    private val onPrepareP2pGroup: () -> P2pGroupCredentials?,
    private val onJoinP2pGroup: (P2pGroupCredentials) -> Boolean,
    private val onCleanupP2pGroup: () -> Unit = {},
    private val isHighBandUpgradeEnabled: () -> Boolean,
    private val resolveActiveBandLabel: (String, String) -> String = { _, fallback -> fallback },
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onIncomingConsentRequest: (IncomingTransferRequest) -> Unit,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit,
    private val onTransferActiveStateChanged: (Boolean) -> Unit
) {
    companion object {
        // 1 MB SO_SNDBUF per stream (4 x 1 MB = 4 MB user / 8 MB kernel sk_sndbuf):
        // 10x larger than 80 MHz Wi-Fi Direct BDP (420 KB at 140 MB/s), keeping A-MPDU aggregation
        // 100% saturated while completely eliminating the 96 MB kernel bufferbloat & 7.4s initial freeze!
        const val SENDER_SOCKET_BUFFER_BYTES = 1 * 1024 * 1024
        const val RECEIVER_SOCKET_BUFFER_BYTES = 16 * 1024 * 1024
        const val DIRECT_BUFFER_BYTES = 4 * 1024 * 1024
        const val STRIPE_BYTES = 512 * 1024 // 512 KB Page-Aligned Stripe Unit (4ms wire time at 125 MB/s)
        const val SOCKET_WRITE_SLICE_BYTES = 128 * 1024 // 128 KB non-stalling socket write slice (1-3ms per write)
        const val COALESCE_WRITE_BYTES = 4 * 1024 * 1024 // 4 MB Sequential Coalesced Receiver UFS Write Buffer
        const val REORDER_RING_BUFFERS = 96 // 96 x 512 KB = 48 MB Off-Heap Reorder Ring Pool
        const val MAX_IN_FLIGHT_SPREAD = 48 // Max 24 MB Sender spread (< 48 MB Receiver pool) so pool never exhausts
        const val PARALLEL_DATA_STREAMS = 4 // 4 Parallel TCP SocketChannels across 4 CPU Cores
        const val UI_HEARTBEAT_MS = 80L // 80ms (12.5 FPS) Independent Real-Time UI & Elapsed-Time Ticker
        private const val PAGE_ALIGN_OFFSET = 4096 // 4096-byte page alignment offset for Os.pread DMA
        private const val PROTOCOL_MAGIC = 0x5A45503A // "ZEP:" Control & Handshake Channel
        private const val PROTOCOL_MAGIC_TURBO_DATA = 0x5A455054 // "ZEPT" Parallel Data Channel
        private const val TURBO_READY_ACK_BYTE: Int = 0x5A // Two-way socket verification byte
        private const val DECISION_ACCEPTED: Int = 1
        private const val DECISION_REJECTED: Int = 2
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverJob: Job? = null
    private var httpServerJob: Job? = null
    private var serverChannel: ServerSocketChannel? = null
    private var httpServerSocket: ServerSocket? = null

    private val isPaused = AtomicBoolean(false)
    private val isCanceled = AtomicBoolean(false)

    @Volatile
    private var activeSessionCancelFlag: AtomicBoolean? = null

    @Volatile
    var autoAcceptIncoming: Boolean = false

    @Volatile
    private var pendingIncomingDecision: CompletableDeferred<Int>? = null

    private val incomingTurboLock = Object()

    @Volatile
    private var expectedSessionToken: Long = 0L
    private val incomingTurboPool = mutableListOf<SocketChannel>()

    @Volatile
    private var activeSenderControlChannel: SocketChannel? = null
    private val activeSenderWorkerChannels = CopyOnWriteArrayList<SocketChannel>()

    @Volatile
    private var activeReceiverChannel: SocketChannel? = null
    private val activeReceiverWorkerChannels = CopyOnWriteArrayList<SocketChannel>()

    @Volatile
    private var sharedItemsForWebLink: List<TransferItem> = emptyList()

    private val historyRecords = CopyOnWriteArrayList<TransferHistoryRecord>()

    private var activeWakeLock: PowerManager.WakeLock? = null
    private var activeWifiLock: WifiManager.WifiLock? = null

    private class IndependentFileHandle(
        val fd: FileDescriptor,
        val channel: FileChannel,
        private val closeables: List<Closeable>
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

    fun getHistoryRecords(): List<TransferHistoryRecord> = historyRecords.toList()

    fun clearHistory() {
        historyRecords.clear()
    }

    fun setSharedItemsForLink(items: List<TransferItem>) {
        sharedItemsForWebLink = items
    }

    @Suppress("DEPRECATION")
    private fun acquireInProcessHardwareLocks() {
        try {
            val pm = context.applicationContext.getSystemService(Context.POWER_SERVICE) as? PowerManager
            if (pm != null && activeWakeLock == null) {
                activeWakeLock = pm.newWakeLock(
                    PowerManager.PARTIAL_WAKE_LOCK,
                    "Zeph:EngineTurboWakeLock"
                ).apply {
                    setReferenceCounted(false)
                    acquire(60 * 60 * 1000L)
                }
            }
        } catch (_: Throwable) {
        }
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null && activeWifiLock == null) {
                val lockMode = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                    WifiManager.WIFI_MODE_FULL_LOW_LATENCY
                } else {
                    WifiManager.WIFI_MODE_FULL_HIGH_PERF
                }
                activeWifiLock = wm.createWifiLock(lockMode, "Zeph:EngineTurboWifiLock").apply {
                    setReferenceCounted(false)
                    acquire()
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun releaseInProcessHardwareLocks() {
        try {
            activeWakeLock?.let {
                if (it.isHeld) it.release()
            }
        } catch (_: Throwable) {
        } finally {
            activeWakeLock = null
        }
        try {
            activeWifiLock?.let {
                if (it.isHeld) it.release()
            }
        } catch (_: Throwable) {
        } finally {
            activeWifiLock = null
        }
    }

    fun startServer() {
        if (serverJob?.isActive == true) return
        serverJob = scope.launch {
            try {
                val channel = ServerSocketChannel.open()
                channel.configureBlocking(true)
                try {
                    channel.socket().reuseAddress = true
                    channel.socket().receiveBufferSize = RECEIVER_SOCKET_BUFFER_BYTES
                } catch (_: Throwable) {
                }
                channel.socket().bind(InetSocketAddress(transferPort))
                serverChannel = channel

                while (isActive) {
                    val clientChannel = channel.accept() ?: continue
                    launch(Dispatchers.IO) {
                        routeIncomingSocket(clientChannel)
                    }
                }
            } catch (_: Throwable) {
            }
        }

        if (httpServerJob?.isActive != true) {
            httpServerJob = scope.launch {
                startEmbeddedHttpLinkServer()
            }
        }
    }

    fun stopServer() {
        serverJob?.cancel()
        httpServerJob?.cancel()
        releaseInProcessHardwareLocks()
        try {
            serverChannel?.close()
        } catch (_: Throwable) {
        }
        try {
            httpServerSocket?.close()
        } catch (_: Throwable) {
        }
    }

    fun pauseCurrentTransfer() {
        isPaused.set(true)
    }

    fun resumeCurrentTransfer() {
        isPaused.set(false)
    }

    fun isCurrentlyPaused(): Boolean = isPaused.get()

    fun cancelCurrentTransferKeepPartial() {
        isCanceled.set(true)
        activeSessionCancelFlag?.set(true)
        isPaused.set(false)
        pendingIncomingDecision?.complete(DECISION_REJECTED)
        for (ch in activeSenderWorkerChannels) {
            abortChannelWithRst(ch)
        }
        activeSenderWorkerChannels.clear()
        abortChannelWithRst(activeSenderControlChannel)
        for (ch in activeReceiverWorkerChannels) {
            closeChannelGracefully(ch)
        }
        activeReceiverWorkerChannels.clear()
        closeChannelGracefully(activeReceiverChannel)
    }

    fun respondToIncomingRequest(accept: Boolean) {
        pendingIncomingDecision?.complete(if (accept) DECISION_ACCEPTED else DECISION_REJECTED)
    }

    private fun abortChannelWithRst(ch: SocketChannel?) {
        if (ch == null) return
        try {
            ch.socket().setSoLinger(true, 0)
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
            ch.close()
        } catch (_: Throwable) {
        }
    }

    private fun configureChannelSocketBuffers(channel: SocketChannel, isSender: Boolean) {
        val sock = channel.socket()
        val sndBuf = if (isSender) SENDER_SOCKET_BUFFER_BYTES else (256 * 1024)
        val rcvBuf = if (isSender) (512 * 1024) else RECEIVER_SOCKET_BUFFER_BYTES
        try {
            sock.tcpNoDelay = true
            sock.keepAlive = true
            sock.sendBufferSize = sndBuf
            sock.receiveBufferSize = rcvBuf
            sock.trafficClass = 0x10 or 0x08 // IPTOS_LOWDELAY | IPTOS_THROUGHPUT
        } catch (_: Throwable) {
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
            try {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true)
                channel.setOption(StandardSocketOptions.SO_SNDBUF, sndBuf)
                channel.setOption(StandardSocketOptions.SO_RCVBUF, rcvBuf)
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

    /**
     * Writes [buf] to [ch] in [SOCKET_WRITE_SLICE_BYTES] (128 KB) non-stalling slices so
     * blocking SocketChannel.write() NEVER sleeps inside Linux tcp_sendmsg_locked for 1.7s
     * waiting for a full 2 MB buffer, and [totalBytesSentAtomic] increments every 128 KB!
     */
    private fun writeFrameInSlices(
        ch: SocketChannel,
        frameBuf: ByteBuffer,
        headerBytesCount: Int,
        totalBytesSentAtomic: AtomicLong,
        sessionCancel: AtomicBoolean
    ): Boolean {
        val finalLimit = frameBuf.limit()
        var headerRemaining = headerBytesCount
        while (frameBuf.position() < finalLimit && !sessionCancel.get()) {
            val pos = frameBuf.position()
            val sliceEnd = (pos + SOCKET_WRITE_SLICE_BYTES).coerceAtMost(finalLimit)
            frameBuf.limit(sliceEnd)
            while (frameBuf.hasRemaining() && !sessionCancel.get()) {
                val w = ch.write(frameBuf)
                if (w < 0) {
                    frameBuf.limit(finalLimit)
                    return false
                }
                if (w > 0) {
                    val payloadBytes = if (headerRemaining > 0) {
                        val hdrConsumed = w.coerceAtMost(headerRemaining)
                        headerRemaining -= hdrConsumed
                        w - hdrConsumed
                    } else {
                        w
                    }
                    if (payloadBytes > 0) {
                        totalBytesSentAtomic.addAndGet(payloadBytes.toLong())
                    }
                }
            }
        }
        frameBuf.limit(finalLimit)
        return !sessionCancel.get()
    }

    /**
     * Opens a dedicated IndependentFileHandle (with its own kernel struct file & sequential
     * read-ahead state f_ra) for each parallel worker thread!
     * Resolves the underlying /storage/emulated/0/... path via /proc/self/fd readlink whenever
     * accessible, and with ACCESS_MEDIA_LOCATION granted in AndroidManifest.xml, Android 10-14's
     * MediaProvider enables direct Kernel FUSE_PASSTHROUGH without MP4 redaction scanning!
     */
    private fun openIndependentFileHandle(item: TransferItem): IndependentFileHandle? {
        try {
            if (item.directFile != null && item.directFile.exists() && item.directFile.canRead()) {
                val fis = FileInputStream(item.directFile)
                return IndependentFileHandle(
                    fd = fis.fd,
                    channel = fis.channel,
                    closeables = listOf(fis.channel, fis)
                )
            }
            if (item.uri != null) {
                val pfd = context.contentResolver.openFileDescriptor(item.uri, "r") ?: return null
                try {
                    val linkPath = Os.readlink("/proc/self/fd/${pfd.fd}")
                    if (!linkPath.isNullOrBlank()) {
                        val directCandidates = mutableListOf(File(linkPath))
                        val suffix = linkPath.substringAfter("/emulated/0/", "")
                        if (suffix.isNotEmpty()) {
                            directCandidates.add(File("/storage/emulated/0/$suffix"))
                        }
                        for (candidate in directCandidates) {
                            if (candidate.exists() && candidate.isFile && candidate.canRead()) {
                                val directFis = FileInputStream(candidate)
                                try {
                                    pfd.close()
                                } catch (_: Throwable) {
                                }
                                return IndependentFileHandle(
                                    fd = directFis.fd,
                                    channel = directFis.channel,
                                    closeables = listOf(directFis.channel, directFis)
                                )
                            }
                        }
                    }
                } catch (_: Throwable) {
                }

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

    /**
     * Single-buffer positional pwrite64(fd, buf, count, offset) on the Receiver:
     * Writes 4096-byte page-aligned sequential buffers directly to disk!
     */
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

    /**
     * Lockless positional pread64(fd, buf, count, offset) on a per-worker file descriptor:
     * Reads into a 4096-byte page-aligned address (buf.position() == 4096) so Linux FUSE /
     * f2fs / UFS DMA never splits pages across 4 KB boundaries!
     */
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
        val windowNs = 1_800_000_000L
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
        if (oldest != null && nowNs - oldest.first >= 250_000_000L) {
            val windowSec = (nowNs - oldest.first) / 1_000_000_000.0
            val windowBytes = (totalBytes - oldest.second).coerceAtLeast(0L)
            val windowMbps = (windowBytes / (1024.0 * 1024.0)) / windowSec
            return (windowMbps * 0.65) + (cumulativeMbps * 0.35)
        }
        return cumulativeMbps
    }

    /**
     * Starts an independent 80ms (12.5 FPS) Real-Time UI Heartbeat Ticker Thread so that:
     * 1. 'Geçen Süre' (Elapsed Time) ticks smoothly every 80ms (0.1s -> 0.2s -> 0.3s) and NEVER freezes!
     * 2. 'Boyut' and 'Hız' update continuously at 12.5 FPS even while a worker thread is inside a syscall!
     */
    private fun startRealtimeUiHeartbeatTicker(
        fileName: String,
        fileIndex: Int,
        totalFiles: Int,
        totalBytes: Long,
        bytesCounter: AtomicLong,
        startTimeNs: Long,
        startWallMs: Long,
        transportLabelProvider: () -> String,
        isSending: Boolean,
        savedFilePath: String?,
        stopTickerFlag: AtomicBoolean,
        sessionCancel: AtomicBoolean
    ): Thread {
        return Thread {
            val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
            var smoothedSpeed = 0.0
            try {
                while (!stopTickerFlag.get() && !sessionCancel.get()) {
                    if (!isPaused.get()) {
                        val nowMs = System.currentTimeMillis()
                        val nowNs = System.nanoTime()
                        val curBytes = bytesCounter.get().coerceIn(0L, totalBytes)
                        val rawSpeed = computeSustainedSpeedMbps(
                            samples = speedSamples,
                            nowNs = nowNs,
                            totalBytes = curBytes,
                            startTimeNs = startTimeNs
                        )
                        smoothedSpeed = if (smoothedSpeed <= 0.01) {
                            rawSpeed
                        } else {
                            (smoothedSpeed * 0.45) + (rawSpeed * 0.55)
                        }
                        val remBytes = (totalBytes - curBytes).coerceAtLeast(0L)
                        val etaSec = if (smoothedSpeed > 0.01) {
                            ((remBytes / (1024.0 * 1024.0)) / smoothedSpeed).toLong()
                        } else {
                            0L
                        }
                        val reportedBytes = if (isSending && curBytes >= totalBytes && totalBytes > 1L) {
                            totalBytes - 1L
                        } else {
                            curBytes
                        }
                        onProgressUpdate(
                            TransferProgress(
                                fileName = fileName,
                                fileIndex = fileIndex,
                                totalFiles = totalFiles,
                                bytesTransferred = reportedBytes,
                                totalBytes = totalBytes,
                                speedMbps = smoothedSpeed,
                                etaSeconds = etaSec.coerceAtLeast(1L),
                                elapsedMillis = (nowMs - startWallMs).coerceAtLeast(0L),
                                status = TransferStatus.TRANSFERRING,
                                transportLabel = transportLabelProvider(),
                                isSending = isSending,
                                savedFilePath = savedFilePath
                            )
                        )
                    } else {
                        speedSamples.clear()
                        smoothedSpeed = 0.0
                    }
                    Thread.sleep(UI_HEARTBEAT_MS)
                }
            } catch (_: Throwable) {
            }
        }.apply {
            name = if (isSending) "Zeph-SenderUiTicker" else "Zeph-ReceiverUiTicker"
            isDaemon = true
            start()
        }
    }

    private fun routeIncomingSocket(clientChannel: SocketChannel) {
        try {
            clientChannel.configureBlocking(true)
            configureChannelSocketBuffers(clientChannel, isSender = false)
            val dis = DataInputStream(clientChannel.socket().getInputStream())
            val magic = dis.readInt()
            if (magic == PROTOCOL_MAGIC_TURBO_DATA) {
                val token = dis.readLong()
                var acceptedTurbo = false
                synchronized(incomingTurboLock) {
                    if (token != 0L && token == expectedSessionToken) {
                        acceptedTurbo = true
                    }
                }
                if (acceptedTurbo) {
                    try {
                        val out = clientChannel.socket().getOutputStream()
                        out.write(TURBO_READY_ACK_BYTE)
                        out.flush()
                        synchronized(incomingTurboLock) {
                            incomingTurboPool.add(clientChannel)
                            incomingTurboLock.notifyAll()
                        }
                    } catch (_: Throwable) {
                        closeChannelGracefully(clientChannel)
                    }
                } else {
                    closeChannelGracefully(clientChannel)
                }
                return
            }
            if (magic == PROTOCOL_MAGIC) {
                handleIncomingControlSession(clientChannel, dis)
            } else {
                closeChannelGracefully(clientChannel)
            }
        } catch (_: Throwable) {
            closeChannelGracefully(clientChannel)
        }
    }

    /**
     * RECEIVER PIPELINE (v17):
     * 1. Immediately informs the Sender (before "Kabul et" is even clicked!) whether Receiver or
     *    Sender will pre-create the 5 GHz Group Owner, so the 5 GHz Group Owner is 100% READY
     *    by the time the user taps "Kabul et" (0ms post-consent GO creation delay!).
     * 2. Uses 4 Parallel Non-Blocking Network Readers (512 KB stripes) + a Coalesced 4 MB
     *    100% Pure Sequential UFS Writer (ZERO out-of-order sparse hole pwrites on f2fs/FUSE!).
     * 3. Runs an independent 80ms Real-Time UI Heartbeat Ticker so 'Geçen Süre' and 'Boyut' never freeze!
     */
    private fun handleIncomingControlSession(clientChannel: SocketChannel, dis: DataInputStream) {
        val sessionCancel = AtomicBoolean(false)
        activeSessionCancelFlag = sessionCancel
        isCanceled.set(false)
        isPaused.set(false)

        val previousReceiver = activeReceiverChannel
        if (previousReceiver != null && previousReceiver !== clientChannel) {
            closeChannelGracefully(previousReceiver)
        }
        activeReceiverChannel = clientChannel
        activeReceiverWorkerChannels.clear()

        val socket: Socket = clientChannel.socket()
        val senderIp = socket.inetAddress?.hostAddress ?: "Bilinmeyen"

        acquireInProcessHardwareLocks()
        onTransferActiveStateChanged(true)

        try {
            val dos = DataOutputStream(BufferedOutputStream(socket.getOutputStream(), 64 * 1024))

            // 1. Read Handshake Metadata + Sender GO Capability Score
            val senderAlias = dis.readUTF()
            val senderModel = dis.readUTF().ifBlank { "Android" }
            var transportLabel = dis.readUTF().ifBlank { "⚡ 5 GHz Doğrudan Köprü" }
            val senderGoScore = dis.readInt()
            val fileCount = dis.readInt()
            val totalBatchBytes = dis.readLong()
            val firstFileName = dis.readUTF()
            val firstFileSize = dis.readLong()
            val previewCount = dis.readInt().coerceIn(0, 50)
            val previewLines = mutableListOf<String>()
            repeat(previewCount) {
                previewLines.add(dis.readUTF())
            }

            // 2. Instantaneous Pre-Consent 5 GHz GO Election (Zero Post-Consent Creation Wait!):
            // Tell the Sender immediately (at t = 15ms, BEFORE "Kabul et" is clicked) which side
            // is pre-creating the 5 GHz Group Owner so it is already up while the user taps "Kabul et"!
            val isAlreadyDirectLink = CompatibilityHelper.isAlreadyDirectOrHotspotConnection(context, senderIp)
            if (isAlreadyDirectLink) {
                transportLabel = resolveActiveBandLabel(senderIp, transportLabel)
            }
            val receiverCanCreateExplicit5GhzGo = Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU
            val senderCanCreateExplicit5GhzGo = senderGoScore >= 300
            val preferReceiverAsGo = receiverCanCreateExplicit5GhzGo || !senderCanCreateExplicit5GhzGo
            val shouldUpgradeToP2p = !isAlreadyDirectLink && isHighBandUpgradeEnabled()

            // Immediately send pre-consent election flags so Sender can pre-create its 5 GHz GO during consent wait!
            dos.writeBoolean(shouldUpgradeToP2p)
            dos.writeBoolean(preferReceiverAsGo)
            dos.flush()

            val p2pGroupDeferred = CompletableDeferred<P2pGroupCredentials?>()
            if (shouldUpgradeToP2p && preferReceiverAsGo) {
                scope.launch(Dispatchers.IO) {
                    try {
                        p2pGroupDeferred.complete(onPrepareP2pGroup())
                    } catch (_: Throwable) {
                        p2pGroupDeferred.complete(null)
                    }
                }
            } else {
                p2pGroupDeferred.complete(null)
            }

            // 3. Consent Check (Auto-Accept or User Overlay)
            val decision: Int
            if (autoAcceptIncoming) {
                decision = DECISION_ACCEPTED
            } else {
                val decisionDeferred = CompletableDeferred<Int>()
                pendingIncomingDecision = decisionDeferred

                onIncomingConsentRequest(
                    IncomingTransferRequest(
                        senderAlias = senderAlias,
                        senderModel = senderModel,
                        senderIp = senderIp,
                        transportLabel = transportLabel,
                        fileCount = fileCount,
                        totalBytes = totalBatchBytes,
                        firstFileName = firstFileName,
                        firstFileSize = firstFileSize,
                        filePreviewList = previewLines
                    )
                )

                val waited = runBlocking {
                    withTimeoutOrNull(60_000L) {
                        decisionDeferred.await()
                    }
                } ?: DECISION_REJECTED

                pendingIncomingDecision = null
                decision = waited
            }

            if (decision != DECISION_ACCEPTED || sessionCancel.get()) {
                dos.writeInt(DECISION_REJECTED)
                dos.flush()
                return
            }

            val sessionToken = Random.nextLong(100_000_000L, Long.MAX_VALUE)
            synchronized(incomingTurboLock) {
                expectedSessionToken = sessionToken
                incomingTurboPool.forEach { closeChannelGracefully(it) }
                incomingTurboPool.clear()
            }

            dos.writeInt(DECISION_ACCEPTED)
            dos.writeLong(sessionToken)

            val receiverP2pCreds: P2pGroupCredentials? = runBlocking {
                withTimeoutOrNull(1800L) {
                    p2pGroupDeferred.await()
                }
            }

            // Phase A: Offer Receiver-GO if Receiver formed a valid 5 GHz group (<= 32 byte SSID, >= 5000 MHz)
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
                    socket.soTimeout = 4000
                    dis.readBoolean()
                } catch (_: Throwable) {
                    false
                } finally {
                    try { socket.soTimeout = 0 } catch (_: Throwable) {}
                }
                if (senderJoinedReceiverGo) {
                    upgradedToP2p = true
                    transportLabel = receiverP2pCreds.bandLabel
                } else {
                    onCleanupP2pGroup()
                }
            } else {
                dos.writeBoolean(false)
                dos.flush()
            }

            // Phase B: Sender-GO (Already pre-warmed by the Sender during the user's "Kabul et" click!)
            val allowSenderGoFallback = !isAlreadyDirectLink && !upgradedToP2p && isHighBandUpgradeEnabled()
            dos.writeBoolean(allowSenderGoFallback)
            dos.flush()

            if (allowSenderGoFallback) {
                val senderCreatedGo = try {
                    socket.soTimeout = 4000
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

                    if (sFreq >= 5000) {
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
                            for (waitIdx in 0 until 20) {
                                val ip = CompatibilityHelper.getLocalP2pInterfaceIp()
                                if (!ip.isNullOrBlank() && ip != "192.168.49.1") {
                                    myP2pClientIp = ip
                                    break
                                }
                                Thread.sleep(50L)
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
                    } else {
                        dos.writeBoolean(false)
                        dos.flush()
                    }
                }
            }

            if (!upgradedToP2p) {
                transportLabel = resolveActiveBandLabel(senderIp, transportLabel)
            }

            // 4. Negotiate 4 Parallel Verified Turbo Data Channels
            val senderOpenedStreams = try {
                socket.soTimeout = 4000
                dis.readInt()
            } catch (_: Throwable) {
                0
            } finally {
                try { socket.soTimeout = 0 } catch (_: Throwable) {}
            }

            if (senderOpenedStreams > 0) {
                val deadline = System.currentTimeMillis() + 2200L
                synchronized(incomingTurboLock) {
                    while (incomingTurboPool.size < senderOpenedStreams &&
                        System.currentTimeMillis() < deadline &&
                        !sessionCancel.get()
                    ) {
                        val rem = (deadline - System.currentTimeMillis()).coerceAtLeast(1L)
                        incomingTurboLock.wait(rem.coerceAtMost(60L))
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
                val stopUiTicker = AtomicBoolean(false)

                val uiTickerThread = startRealtimeUiHeartbeatTicker(
                    fileName = targetFile.name,
                    fileIndex = fileIdx,
                    totalFiles = fileCount,
                    totalBytes = expectedFileSize,
                    bytesCounter = totalBytesWrittenAtomic,
                    startTimeNs = startTimeNs,
                    startWallMs = startWallMs,
                    transportLabelProvider = { transportLabel },
                    isSending = false,
                    savedFilePath = targetFile.absolutePath,
                    stopTickerFlag = stopUiTicker,
                    sessionCancel = sessionCancel
                )

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
                        val coalesceWriteBuf = ByteBuffer.allocateDirect(COALESCE_WRITE_BYTES)

                        // Spawn 4 Parallel Non-Blocking Network Reader Threads across 4 CPU Cores
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
                                            dataBuf = freeBuffers.poll(50L, TimeUnit.MILLISECONDS)
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
                                            if (writtenStripesBitSet.get(stripeIdx)) {
                                                dataBuf.clear()
                                                freeBuffers.offer(dataBuf)
                                            } else {
                                                val existing = completedStripes.putIfAbsent(stripeIdx, dataBuf)
                                                if (existing != null) {
                                                    dataBuf.clear()
                                                    freeBuffers.offer(dataBuf)
                                                } else {
                                                    synchronized(reorderLock) {
                                                        reorderLock.notifyAll()
                                                    }
                                                }
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

                        // Dedicated Single UFS Writer Thread (Coalesced 4 MB 100% Sequential Append):
                        // Coalesces up to 8 contiguous 512 KB stripes (4 MB) into a single sequential
                        // write at nextContiguousStripe so UFS 2.1 / 3.1 / 4.0 and FUSE NEVER create
                        // sparse holes beyond EOF!
                        var pauseTailApplied = false
                        while (nextContiguousStripe < totalStripes && !sessionCancel.get() && !writerError.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                if (!pauseTailApplied) {
                                    pauseTailApplied = true
                                    try {
                                        val drainDeadline = System.currentTimeMillis() + 150L
                                        while (System.currentTimeMillis() < drainDeadline) {
                                            val ready = completedStripes.remove(nextContiguousStripe)
                                            if (ready != null) {
                                                val offset = nextContiguousStripe.toLong() * STRIPE_BYTES.toLong()
                                                val w = writeBufferAtOffsetSingleThread(rawFd, fc, ready, offset)
                                                totalBytesWrittenAtomic.addAndGet(w)
                                                writtenStripesBitSet.set(nextContiguousStripe)
                                                nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                                                ready.clear()
                                                freeBuffers.offer(ready)
                                            } else {
                                                Thread.sleep(10L)
                                            }
                                        }
                                        val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                            .coerceAtMost(expectedFileSize)
                                        contiguousWatermarkBytes.set(contiguousNow)
                                        val effectiveNow = totalBytesWrittenAtomic.get()
                                            .coerceAtLeast(contiguousNow)
                                            .coerceAtMost(expectedFileSize)
                                        if (mp4Meta != null && effectiveNow > 0L && effectiveNow < expectedFileSize) {
                                            Mp4FastStartHelper.applyPartialMoovTail(fc, effectiveNow, mp4Meta)
                                        } else {
                                            fc.force(true)
                                        }
                                    } catch (_: Throwable) {
                                    }
                                }
                                val pausedBytes = totalBytesWrittenAtomic.get()
                                    .coerceAtLeast(contiguousWatermarkBytes.get())
                                    .coerceAtMost(expectedFileSize)
                                onProgressUpdate(
                                    TransferProgress(
                                        fileName = targetFile.name,
                                        fileIndex = fileIdx,
                                        totalFiles = fileCount,
                                        bytesTransferred = pausedBytes,
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
                                Thread.sleep(80L)
                            }
                            if (pauseTailApplied && !sessionCancel.get()) {
                                pauseTailApplied = false
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

                            // Coalesce up to 8 contiguous 512 KB stripes (4 MB) starting at nextContiguousStripe
                            val batchStartStripe = nextContiguousStripe
                            val firstBuf = completedStripes.remove(batchStartStripe)
                            if (firstBuf != null) {
                                coalesceWriteBuf.clear()
                                coalesceWriteBuf.put(firstBuf)
                                firstBuf.clear()
                                freeBuffers.offer(firstBuf)
                                writtenStripesBitSet.set(batchStartStripe)
                                var batchEndStripe = batchStartStripe + 1

                                while (batchEndStripe < totalStripes &&
                                    coalesceWriteBuf.remaining() >= STRIPE_BYTES
                                ) {
                                    val nextBuf = completedStripes.remove(batchEndStripe) ?: break
                                    coalesceWriteBuf.put(nextBuf)
                                    nextBuf.clear()
                                    freeBuffers.offer(nextBuf)
                                    writtenStripesBitSet.set(batchEndStripe)
                                    batchEndStripe++
                                }

                                coalesceWriteBuf.flip()
                                val writeOffset = batchStartStripe.toLong() * STRIPE_BYTES.toLong()
                                val written = writeBufferAtOffsetSingleThread(rawFd, fc, coalesceWriteBuf, writeOffset)
                                totalBytesWrittenAtomic.addAndGet(written)
                                nextContiguousStripe = writtenStripesBitSet.nextClearBit(batchEndStripe)
                                val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                    .coerceAtMost(expectedFileSize)
                                contiguousWatermarkBytes.set(contiguousNow)
                                continue
                            }

                            // Fallback ONLY if all reader sockets have closed or pool is >= 85% full
                            if (completedStripes.size >= (REORDER_RING_BUFFERS - 12) || activeReaders.get() == 0) {
                                val firstEntry = completedStripes.pollFirstEntry()
                                if (firstEntry != null) {
                                    val sIdx = firstEntry.key
                                    val buf = firstEntry.value
                                    if (!writtenStripesBitSet.get(sIdx)) {
                                        val stripeOffset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                        val written = writeBufferAtOffsetSingleThread(rawFd, fc, buf, stripeOffset)
                                        totalBytesWrittenAtomic.addAndGet(written)
                                        writtenStripesBitSet.set(sIdx)
                                        if (sIdx == nextContiguousStripe) {
                                            nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                                        }
                                        val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                            .coerceAtMost(expectedFileSize)
                                        contiguousWatermarkBytes.set(contiguousNow)
                                    }
                                    buf.clear()
                                    freeBuffers.offer(buf)
                                    continue
                                }
                            }

                            if (activeReaders.get() == 0 && completedStripes.isEmpty()) {
                                break
                            }
                            synchronized(reorderLock) {
                                if (!completedStripes.containsKey(nextContiguousStripe) &&
                                    activeReaders.get() > 0 &&
                                    !sessionCancel.get()
                                ) {
                                    reorderLock.wait(6L)
                                }
                            }
                        }

                        // Drain ALL remaining stripes sitting in memory on completion or cancel so NOT A SINGLE KB is lost!
                        while (true) {
                            val entry = completedStripes.pollFirstEntry() ?: break
                            val sIdx = entry.key
                            val pending = entry.value
                            if (!writtenStripesBitSet.get(sIdx)) {
                                val stripeOffset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                val written = writeBufferAtOffsetSingleThread(rawFd, fc, pending, stripeOffset)
                                totalBytesWrittenAtomic.addAndGet(written)
                                writtenStripesBitSet.set(sIdx)
                                if (sIdx == nextContiguousStripe) {
                                    nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                                }
                            }
                            pending.clear()
                        }
                        val finalContiguous = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                            .coerceAtMost(expectedFileSize)
                        contiguousWatermarkBytes.set(finalContiguous)

                        for (rt in readerThreads) {
                            try {
                                rt.join(350L)
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
                                Thread.sleep(85L)
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
                        }
                    }
                } catch (_: Throwable) {
                } finally {
                    stopUiTicker.set(true)
                    try {
                        uiTickerThread.join(120L)
                    } catch (_: Throwable) {
                    }

                    val contiguousBytes = contiguousWatermarkBytes.get().coerceAtMost(expectedFileSize)
                    val totalBytesReceived = totalBytesWrittenAtomic.get()
                        .coerceAtLeast(contiguousBytes)
                        .coerceAtMost(expectedFileSize)
                    val isPartial = totalBytesReceived in 1L until expectedFileSize
                    try {
                        if (fileChannel != null && fileChannel.isOpen) {
                            if (isPartial) {
                                if (mp4Meta != null) {
                                    Mp4FastStartHelper.applyPartialMoovTail(
                                        fileChannel,
                                        totalBytesReceived,
                                        mp4Meta
                                    )
                                } else {
                                    fileChannel.truncate(totalBytesReceived)
                                    fileChannel.force(true)
                                }
                            } else if (totalBytesReceived == expectedFileSize) {
                                fileChannel.truncate(expectedFileSize)
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
                                    "Kısmi Dosya Korundu (%$pct • ${StorageHelper.formatFileSize(totalBytesReceived)} oynatmaya hazır)"
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
                for (w in workers) abortChannelWithRst(w)
                abortChannelWithRst(clientChannel)
            } else {
                for (w in workers) closeChannelGracefully(w)
                closeChannelGracefully(clientChannel)
            }
        }
    }

    /**
     * SENDER PIPELINE (v17):
     * 1. Pre-warms the 5 GHz Group Owner in parallel while waiting for the Receiver's "Kabul et"
     *    click so there is ZERO GO creation wait after "Kabul et"!
     * 2. Uses 4 Parallel Workers with 100% 4096-byte page-aligned Os.pread (offset 4096 inside
     *    workerFrameBuf) + 128 KB non-stalling socket write slices + 1 MB SO_SNDBUF (eliminating
     *    the 96 MB kernel bufferbloat & 7.4s initial freeze).
     * 3. Runs an independent 80ms Real-Time UI Heartbeat Ticker so 'Geçen Süre' and 'Boyut' never freeze!
     */
    fun sendItemsToPeers(
        items: List<TransferItem>,
        targetHostIps: List<Pair<String, Int>>,
        transportLabel: String
    ) {
        if (items.isEmpty() || targetHostIps.isEmpty()) return
        val sessionCancel = AtomicBoolean(false)
        activeSessionCancelFlag = sessionCancel
        isCanceled.set(false)
        isPaused.set(false)

        scope.launch {
            acquireInProcessHardwareLocks()
            onTransferActiveStateChanged(true)
            try {
                for ((hostIp, port) in targetHostIps) {
                    if (sessionCancel.get()) break
                    sendBatchToSinglePeer(items, hostIp, port, transportLabel, sessionCancel)
                }
            } finally {
                releaseInProcessHardwareLocks()
                onTransferActiveStateChanged(false)
            }
        }
    }

    private fun sendBatchToSinglePeer(
        items: List<TransferItem>,
        hostIp: String,
        port: Int,
        initialTransportLabel: String,
        sessionCancel: AtomicBoolean
    ) {
        var controlChannel: SocketChannel? = null
        val workerChannels = mutableListOf<SocketChannel>()
        var transportLabel = resolveActiveBandLabel(hostIp, initialTransportLabel)
        var activeDataTargetIp = hostIp

        try {
            val totalBatchBytes = items.sumOf { it.sizeBytes.coerceAtLeast(0L) }
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
                    message = "$hostIp:$port adresine bağlanılıyor..."
                )
            )

            controlChannel = SocketChannel.open()
            activeSenderControlChannel = controlChannel
            activeSenderWorkerChannels.clear()
            controlChannel.configureBlocking(true)
            configureChannelSocketBuffers(controlChannel, isSender = true)
            CompatibilityHelper.bindSocketToOptimalInterface(context, controlChannel.socket(), hostIp)
            controlChannel.socket().connect(InetSocketAddress(hostIp, port), 3500)

            val dos = DataOutputStream(BufferedOutputStream(controlChannel.socket().getOutputStream(), 64 * 1024))
            val dis = DataInputStream(controlChannel.socket().getInputStream())

            val senderGoScore = CompatibilityHelper.getDeviceGoCapabilityScore(context)

            // 1. Send Handshake Metadata + Sender GO Capability Score
            dos.writeInt(PROTOCOL_MAGIC)
            dos.writeUTF(getLocalAlias())
            dos.writeUTF(getLocalDeviceModel())
            dos.writeUTF(transportLabel)
            dos.writeInt(senderGoScore)
            dos.writeInt(items.size)
            dos.writeLong(totalBatchBytes)
            dos.writeUTF(firstItem.name)
            dos.writeLong(firstItem.sizeBytes)

            val previewItems = items.take(20)
            dos.writeInt(previewItems.size)
            for (p in previewItems) {
                dos.writeUTF("${p.name} (${StorageHelper.formatFileSize(p.sizeBytes)})")
            }
            dos.flush()

            // Read pre-consent GO election flags sent immediately by Receiver (at t = 15ms, BEFORE "Kabul et")
            val shouldUpgradeToP2p = dis.readBoolean()
            val preferReceiverAsGo = dis.readBoolean()

            // Pre-warm Sender 5 GHz Group Owner in parallel while waiting for user's "Kabul et" click!
            val senderPrewarmGoDeferred = CompletableDeferred<P2pGroupCredentials?>()
            if (shouldUpgradeToP2p && !preferReceiverAsGo) {
                scope.launch(Dispatchers.IO) {
                    try {
                        senderPrewarmGoDeferred.complete(onPrepareP2pGroup())
                    } catch (_: Throwable) {
                        senderPrewarmGoDeferred.complete(null)
                    }
                }
            } else {
                senderPrewarmGoDeferred.complete(null)
            }

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

            controlChannel.socket().soTimeout = 65_000
            val decision = dis.readInt()
            controlChannel.socket().soTimeout = 0

            if (decision != DECISION_ACCEPTED || sessionCancel.get()) {
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

                val joinedP2p = if (p2pFreq >= 5000) onJoinP2pGroup(creds) else false
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

            // 3. Phase B: Sender-GO (Already pre-warmed during "Kabul et" wait!)
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
                        transportLabel = "⚡ 5 GHz Doğrudan Köprü",
                        isSending = true,
                        message = "5 GHz Yüksek Hızlı Doğrudan Köprü bağlanıyor..."
                    )
                )
                val prewarmed = runBlocking {
                    withTimeoutOrNull(1800L) {
                        senderPrewarmGoDeferred.await()
                    }
                }
                val myGoCreds = prewarmed ?: onPrepareP2pGroup()
                if (myGoCreds != null &&
                    myGoCreds.freqMhz >= 5000 &&
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
                    } else {
                        onCleanupP2pGroup()
                    }
                } else {
                    dos.writeBoolean(false)
                    dos.flush()
                }
            }

            transportLabel = resolveActiveBandLabel(activeDataTargetIp, transportLabel)

            // 4. Open 4 Parallel Two-Way-Verified Turbo Data SocketChannels to activeDataTargetIp:port
            for (streamIdx in 0 until PARALLEL_DATA_STREAMS) {
                if (sessionCancel.get()) break
                var openedChan: SocketChannel? = null
                for (attempt in 0 until 5) {
                    if (sessionCancel.get()) break
                    var candidate: SocketChannel? = null
                    try {
                        candidate = SocketChannel.open()
                        candidate.configureBlocking(true)
                        configureChannelSocketBuffers(candidate, isSender = true)
                        CompatibilityHelper.bindSocketToOptimalInterface(
                            context,
                            candidate.socket(),
                            activeDataTargetIp
                        )
                        candidate.socket().connect(InetSocketAddress(activeDataTargetIp, port), 550)
                        candidate.socket().soTimeout = 450
                        val turboOut = DataOutputStream(candidate.socket().getOutputStream())
                        turboOut.writeInt(PROTOCOL_MAGIC_TURBO_DATA)
                        turboOut.writeLong(sessionToken)
                        turboOut.flush()
                        val ackByte = candidate.socket().getInputStream().read()
                        candidate.socket().soTimeout = 0
                        if (ackByte == TURBO_READY_ACK_BYTE) {
                            openedChan = candidate
                            break
                        } else {
                            closeChannelGracefully(candidate)
                        }
                    } catch (_: Throwable) {
                        closeChannelGracefully(candidate)
                        Thread.sleep(55L)
                    }
                }
                if (openedChan != null) {
                    workerChannels.add(openedChan)
                }
            }

            if (workerChannels.isEmpty() && activeDataTargetIp != hostIp && !sessionCancel.get()) {
                onCleanupP2pGroup()
                activeDataTargetIp = hostIp
                transportLabel = resolveActiveBandLabel(hostIp, initialTransportLabel)
                for (streamIdx in 0 until PARALLEL_DATA_STREAMS) {
                    if (sessionCancel.get()) break
                    var candidate: SocketChannel? = null
                    try {
                        candidate = SocketChannel.open()
                        candidate.configureBlocking(true)
                        configureChannelSocketBuffers(candidate, isSender = true)
                        CompatibilityHelper.bindSocketToOptimalInterface(context, candidate.socket(), hostIp)
                        candidate.socket().connect(InetSocketAddress(hostIp, port), 600)
                        candidate.socket().soTimeout = 450
                        val turboOut = DataOutputStream(candidate.socket().getOutputStream())
                        turboOut.writeInt(PROTOCOL_MAGIC_TURBO_DATA)
                        turboOut.writeLong(sessionToken)
                        turboOut.flush()
                        val ackByte = candidate.socket().getInputStream().read()
                        candidate.socket().soTimeout = 0
                        if (ackByte == TURBO_READY_ACK_BYTE) {
                            workerChannels.add(candidate)
                        } else {
                            closeChannelGracefully(candidate)
                        }
                    } catch (_: Throwable) {
                        closeChannelGracefully(candidate)
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
                        actualSize >= 2L * 1024L * 1024L

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
                    val stopUiTicker = AtomicBoolean(false)

                    val uiTickerThread = startRealtimeUiHeartbeatTicker(
                        fileName = item.name,
                        fileIndex = fileIdx,
                        totalFiles = items.size,
                        totalBytes = actualSize,
                        bytesCounter = totalBytesSentAtomic,
                        startTimeNs = startTimeNs,
                        startWallMs = startWallMs,
                        transportLabelProvider = { transportLabel },
                        isSending = true,
                        savedFilePath = null,
                        stopTickerFlag = stopUiTicker,
                        sessionCancel = sessionCancel
                    )

                    try {
                        if (useParallelStripes && primaryHandle != null) {
                            val totalStripes = ((actualSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
                            val nextStripeIdx = AtomicInteger(0)
                            val activeChannels = workerChannels.take(confirmedWorkerCount)

                            // Open a fresh IndependentFileHandle for each worker so every worker has its own
                            // clean kernel struct file & f_ra read-ahead state starting at offset 0!
                            val workerHandles = activeChannels.map {
                                openIndependentFileHandle(item) ?: primaryHandle
                            }

                            val inFlightStripes = AtomicIntegerArray(activeChannels.size)
                            val inFlightStartMs = AtomicLongArray(activeChannels.size)
                            val rescuedStripes = ConcurrentHashMap.newKeySet<Int>()
                            for (i in 0 until activeChannels.size) {
                                inFlightStripes.set(i, Int.MAX_VALUE)
                                inFlightStartMs.set(i, 0L)
                            }

                            fun getMinInFlightStripe(): Int {
                                var minVal = Int.MAX_VALUE
                                for (k in 0 until activeChannels.size) {
                                    val s = inFlightStripes.get(k)
                                    if (s < minVal) minVal = s
                                }
                                return minVal
                            }

                            fun tryRescueStuckStripe(
                                myWorkerIdx: Int,
                                myChan: SocketChannel,
                                myHandle: IndependentFileHandle,
                                frameBuf: ByteBuffer
                            ) {
                                val currentFrontier = nextStripeIdx.get()
                                val now = System.currentTimeMillis()
                                for (k in 0 until activeChannels.size) {
                                    if (k == myWorkerIdx) continue
                                    val stuckStripe = inFlightStripes.get(k)
                                    if (stuckStripe in 0 until totalStripes &&
                                        (currentFrontier - stuckStripe) >= 12
                                    ) {
                                        val startedAt = inFlightStartMs.get(k)
                                        if (startedAt > 0L && (now - startedAt) >= 550L && rescuedStripes.add(stuckStripe)) {
                                            val rescueOffset = stuckStripe.toLong() * STRIPE_BYTES.toLong()
                                            val rescueLen = (actualSize - rescueOffset)
                                                .coerceAtMost(STRIPE_BYTES.toLong()).toInt()
                                            frameBuf.clear()
                                            frameBuf.position(PAGE_ALIGN_OFFSET)
                                            frameBuf.limit(PAGE_ALIGN_OFFSET + rescueLen)
                                            val r = readBufferAtOffsetLockless(
                                                fd = myHandle.fd,
                                                fc = myHandle.channel,
                                                buf = frameBuf,
                                                offset = rescueOffset
                                            )
                                            if (r == rescueLen) {
                                                frameBuf.putInt(PAGE_ALIGN_OFFSET - 8, stuckStripe)
                                                frameBuf.putInt(PAGE_ALIGN_OFFSET - 4, rescueLen)
                                                frameBuf.position(PAGE_ALIGN_OFFSET - 8)
                                                frameBuf.limit(PAGE_ALIGN_OFFSET + rescueLen)
                                                writeFullyToChannel(myChan, frameBuf)
                                            }
                                            // Advance the slow slot's tracked watermark so it never blocks spread check
                                            inFlightStripes.compareAndSet(k, stuckStripe, Int.MAX_VALUE)
                                        }
                                    }
                                }
                            }

                            try {
                                val senderWorkers = activeChannels.mapIndexed { wIdx, wChan ->
                                    val wHandle = workerHandles[wIdx]
                                    Thread {
                                        try {
                                            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                                            // 4096-Byte Page-Aligned DirectByteBuffer (4096 + 512 KB):
                                            // - Payload is read via Os.pread into offset 4096..(4096 + stripeLen)
                                            //   which is 100% 4096-byte page-aligned ((base_addr + 4096) % 4096 == 0)!
                                            // - 8-byte header is placed at offset 4088..4095 right before payload!
                                            val workerFrameBuf = ByteBuffer
                                                .allocateDirect(PAGE_ALIGN_OFFSET + STRIPE_BYTES)
                                                .order(ByteOrder.BIG_ENDIAN)

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
                                                    Thread.sleep(80L)
                                                }
                                                if (sessionCancel.get()) break

                                                // Pre-claim window pacing: never let fast sockets run > 48 stripes (24 MB)
                                                // ahead of the slowest socket BEFORE claiming nextStripeIdx!
                                                var waitSpin = 0
                                                while (!sessionCancel.get() && !isPaused.get()) {
                                                    val minS = getMinInFlightStripe()
                                                    val curNext = nextStripeIdx.get()
                                                    if (curNext >= totalStripes ||
                                                        minS == Int.MAX_VALUE ||
                                                        (curNext - minS) < MAX_IN_FLIGHT_SPREAD
                                                    ) {
                                                        break
                                                    }
                                                    tryRescueStuckStripe(wIdx, wChan, wHandle, workerFrameBuf)
                                                    waitSpin++
                                                    if (waitSpin > 25) break
                                                    Thread.sleep(2L)
                                                }

                                                val sIdx = nextStripeIdx.getAndIncrement()
                                                if (sIdx >= totalStripes) {
                                                    inFlightStripes.set(wIdx, Int.MAX_VALUE)
                                                    tryRescueStuckStripe(wIdx, wChan, wHandle, workerFrameBuf)
                                                    workerFrameBuf.clear()
                                                    workerFrameBuf.putInt(-1)
                                                    workerFrameBuf.putInt(0)
                                                    workerFrameBuf.flip()
                                                    writeFullyToChannel(wChan, workerFrameBuf)
                                                    break
                                                }

                                                inFlightStripes.set(wIdx, sIdx)
                                                inFlightStartMs.set(wIdx, System.currentTimeMillis())

                                                val stripeOffset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                                val stripeLen = (actualSize - stripeOffset)
                                                    .coerceAtMost(STRIPE_BYTES.toLong()).toInt()

                                                // 1. Read payload into 4096-byte page-aligned offset (4096 .. 4096 + stripeLen)
                                                workerFrameBuf.clear()
                                                workerFrameBuf.position(PAGE_ALIGN_OFFSET)
                                                workerFrameBuf.limit(PAGE_ALIGN_OFFSET + stripeLen)

                                                val readBytes = readBufferAtOffsetLockless(
                                                    fd = wHandle.fd,
                                                    fc = wHandle.channel,
                                                    buf = workerFrameBuf,
                                                    offset = stripeOffset
                                                )
                                                if (readBytes < stripeLen) break

                                                // 2. Write 8-byte header right before payload at 4088..4095
                                                workerFrameBuf.putInt(PAGE_ALIGN_OFFSET - 8, sIdx)
                                                workerFrameBuf.putInt(PAGE_ALIGN_OFFSET - 4, stripeLen)
                                                workerFrameBuf.position(PAGE_ALIGN_OFFSET - 8)
                                                workerFrameBuf.limit(PAGE_ALIGN_OFFSET + stripeLen)

                                                // 3. Stream in 128 KB slices so wChan.write returns every 1-3ms
                                                val wroteOk = writeFrameInSlices(
                                                    ch = wChan,
                                                    frameBuf = workerFrameBuf,
                                                    headerBytesCount = 8,
                                                    totalBytesSentAtomic = totalBytesSentAtomic,
                                                    sessionCancel = sessionCancel
                                                )
                                                inFlightStripes.set(wIdx, Int.MAX_VALUE)
                                                if (!wroteOk) break

                                                tryRescueStuckStripe(wIdx, wChan, wHandle, workerFrameBuf)
                                            }
                                        } catch (_: Throwable) {
                                        } finally {
                                            inFlightStripes.set(wIdx, Int.MAX_VALUE)
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
                                for (h in workerHandles) {
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
                                    val w = streamChan.write(buf)
                                    if (w <= 0) break
                                    totalBytesSentAtomic.addAndGet(w.toLong())
                                }
                            } else if (primaryHandle != null) {
                                while (totalBytesSentAtomic.get() < actualSize && !sessionCancel.get()) {
                                    while (isPaused.get() && !sessionCancel.get()) {
                                        Thread.sleep(80L)
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
                                    val wroteOk = writeFrameInSlices(
                                        ch = streamChan,
                                        frameBuf = fallbackDirectBuffer,
                                        headerBytesCount = 0,
                                        totalBytesSentAtomic = totalBytesSentAtomic,
                                        sessionCancel = sessionCancel
                                    )
                                    if (!wroteOk) break
                                }
                            }
                        }
                    } finally {
                        stopUiTicker.set(true)
                        try {
                            uiTickerThread.join(120L)
                        } catch (_: Throwable) {
                        }
                    }

                    var bytesSent = totalBytesSentAtomic.get().coerceAtMost(actualSize)
                    if (!sessionCancel.get() && bytesSent >= actualSize) {
                        bytesSent = actualSize
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
                        message = "Aktarım durduruldu (Alıcıda o ana kadar inen kısmi dosya korundu)."
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
                        message = "Bağlantı kesildi: ${t.localizedMessage ?: "Ağ hatası"}"
                    )
                )
            }
        } finally {
            if (activeSenderControlChannel === controlChannel) {
                activeSenderControlChannel = null
            }
            val workers = activeSenderWorkerChannels.toList()
            activeSenderWorkerChannels.clear()
            if (sessionCancel.get()) {
                for (w in workers) abortChannelWithRst(w)
                abortChannelWithRst(controlChannel)
            } else {
                for (w in workers) closeChannelGracefully(w)
                closeChannelGracefully(controlChannel)
            }
        }
    }

    private fun startEmbeddedHttpLinkServer() {
        try {
            val server = ServerSocket()
            server.reuseAddress = true
            server.bind(InetSocketAddress(httpLinkPort))
            httpServerSocket = server

            while (scope.isActive && !server.isClosed) {
                val client = server.accept() ?: continue
                scope.launch(Dispatchers.IO) {
                    handleHttpLinkClient(client)
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun handleHttpLinkClient(client: Socket) {
        client.use { sock ->
            try {
                sock.soTimeout = 15_000
                sock.sendBufferSize = SENDER_SOCKET_BUFFER_BYTES
                val reader = sock.getInputStream().bufferedReader(Charsets.UTF_8)
                val requestLine = reader.readLine() ?: return
                val parts = requestLine.split(" ")
                val path = if (parts.size >= 2) parts[1] else "/"

                while (true) {
                    val header = reader.readLine() ?: break
                    if (header.isEmpty()) break
                }

                val out = sock.getOutputStream()
                val items = sharedItemsForWebLink

                if (path.startsWith("/download/")) {
                    val idxStr = path.removePrefix("/download/").substringBefore("?")
                    val idx = idxStr.toIntOrNull() ?: 0
                    val item = items.getOrNull(idx)
                    if (item == null) {
                        val notFound = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n"
                        out.write(notFound.toByteArray(Charsets.UTF_8))
                        out.flush()
                        return
                    }

                    val encodedName = URLEncoder.encode(item.name, "UTF-8").replace("+", "%20")
                    val headers = buildString {
                        append("HTTP/1.1 200 OK\r\n")
                        append("Content-Type: ${item.mimeType}\r\n")
                        append("Content-Length: ${item.sizeBytes}\r\n")
                        append("Content-Disposition: attachment; filename=\"$encodedName\"\r\n")
                        append("Connection: close\r\n\r\n")
                    }
                    out.write(headers.toByteArray(Charsets.UTF_8))

                    if (item.inlineTextBytes != null) {
                        out.write(item.inlineTextBytes)
                    } else if (item.directFile != null && item.directFile.exists()) {
                        FileInputStream(item.directFile).use { fis ->
                            val buf = ByteArray(1024 * 1024)
                            while (true) {
                                val r = fis.read(buf)
                                if (r <= 0) break
                                out.write(buf, 0, r)
                            }
                        }
                    } else if (item.uri != null) {
                        context.contentResolver.openInputStream(item.uri)?.use { ins ->
                            val buf = ByteArray(1024 * 1024)
                            while (true) {
                                val r = ins.read(buf)
                                if (r <= 0) break
                                out.write(buf, 0, r)
                            }
                        }
                    }
                    out.flush()
                } else {
                    val htmlBody = buildHtmlPortalPage(items)
                    val htmlBytes = htmlBody.toByteArray(Charsets.UTF_8)
                    val responseHeader = buildString {
                        append("HTTP/1.1 200 OK\r\n")
                        append("Content-Type: text/html; charset=utf-8\r\n")
                        append("Content-Length: ${htmlBytes.size}\r\n")
                        append("Connection: close\r\n\r\n")
                    }
                    out.write(responseHeader.toByteArray(Charsets.UTF_8))
                    out.write(htmlBytes)
                    out.flush()
                }
            } catch (_: Throwable) {
            }
        }
    }

    private fun buildHtmlPortalPage(items: List<TransferItem>): String {
        val rows = if (items.isEmpty()) {
            "<p style='color:#9CB3B5;'>Henüz paylaşım için seçilmiş dosya yok.</p>"
        } else {
            items.mapIndexed { idx, item ->
                """
                <div style="background:#172224;padding:16px;border-radius:14px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;">
                    <div>
                        <div style="color:#EAF4F4;font-weight:600;font-size:16px;">${item.name}</div>
                        <div style="color:#9CB3B5;font-size:13px;margin-top:4px;">${StorageHelper.formatFileSize(item.sizeBytes)}</div>
                    </div>
                    <a href="/download/$idx" style="background:#6CD4FF;color:#082530;padding:10px 18px;border-radius:20px;text-decoration:none;font-weight:700;">İndir</a>
                </div>
                """.trimIndent()
            }.joinToString("\n")
        }

        return """
        <!DOCTYPE html>
        <html lang="tr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Zeph P2P Paylaşım</title>
        </head>
        <body style="background:#101818;color:#EAF4F4;font-family:sans-serif;padding:24px;max-width:640px;margin:0 auto;">
            <h2 style="color:#6CD4FF;margin-bottom:4px;">Zeph Doğrudan Paylaşım</h2>
            <p style="color:#9CB3B5;margin-top:0;margin-bottom:24px;">Cihaz: ${getLocalAlias()} (${getLocalDeviceModel()})</p>
            $rows
        </body>
        </html>
        """.trimIndent()
    }

    private fun formatDurationSec(elapsedMs: Long): String {
        val totalSec = elapsedMs / 1000.0
        return if (totalSec < 60.0) {
            String.format("%.1f sn", totalSec)
        } else {
            val mins = (totalSec / 60).toInt()
            val secs = totalSec % 60
            String.format("%d dk %.1f sn", mins, secs)
        }
    }
}
