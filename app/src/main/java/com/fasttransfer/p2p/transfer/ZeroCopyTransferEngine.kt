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
import java.util.concurrent.ConcurrentLinkedQueue
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

        // Wi-Fi Direct yükseltmesi (grup kurma + katılma ≈ 2-7 sn) yalnızca kazanç kurulum maliyetini aşıyorsa denenir.
        // Router üzerinden (~40-55 MB/s) doğrudan P2P'ye (~70-95 MB/s) geçişin getirdiği ≈ 10 ms/MB kazançla
        // başa baş noktası ~300 MB'dir; altındaki transferlerde yükseltme net kayıptır, LAN anında başlar.
        const val UPGRADE_MIN_TOTAL_BYTES = 384L * 1024L * 1024L
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
    private var wifiPerfLock: WifiManager.WifiLock? = null

    // Alıcı dosya sonlandırması (truncate/önizleme kuyruğu/ack) bitene kadar stop() bekleyebilsin diye.
    @Volatile
    private var receiverFinalizeLatch: CountDownLatch? = null

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
                    // 20 dk, ~100 GB'lık transferlerde (≈19 dk) CPU kilidini transfer ortasında düşürebiliyordu.
                    // Kilit her oturum sonunda zaten bırakılır; 4 saat yalnızca çökme durumundaki güvenlik tavanıdır.
                    acquire(4 * 60 * 60 * 1000L)
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
            // LOW_LATENCY kilidi yalnızca ekran açık + uygulama ön plandayken aktiftir (AOSP javadoc).
            // HIGH_PERF kilidi ekran kapalıyken/arka planda da Wi-Fi güç tasarrufunu engeller; ikisi birlikte alınır.
            if (wm != null && wifiPerfLock == null) {
                wifiPerfLock = CompatibilityHelper.createHighPerfWifiLock(wm, "Zeph::DirectTransferWifiPerfLock")?.apply {
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
        try {
            wifiPerfLock?.let { if (it.isHeld) it.release() }
            wifiPerfLock = null
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
            // Veri soketleri anında RST ile kesilir: karşı taraf hemen durur (v12 davranışı).
            sWorkers.forEach { abortChannelWithRst(it) }
            rWorkers.forEach { abortChannelWithRst(it) }
            val inDataPhase = sWorkers.isNotEmpty() || rWorkers.isNotEmpty()
            if (!inDataPhase) {
                // Henüz veri fazı yok (onay/müzakere): kontrol kanalındaki bloklu okumaları hemen serbest bırak.
                abortChannelWithRst(sChan)
                abortChannelWithRst(rChan)
            } else {
                // Veri fazında kontrol kanalı bir süre AÇIK kalır: alıcı, diskte KORUNAN bayt sayısını göndericiye
                // bildirebilsin ve iki cihazda da aynı kısmi boyut görünsün. Sonlandırma takılırsa güvenlik ağı:
                try {
                    delay(4000L)
                } catch (_: Throwable) {
                }
                abortChannelWithRst(sChan)
                abortChannelWithRst(rChan)
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

    /**
     * Alıcı tarafı darboğaz analizi (yalnızca gözlem). Yazıcı thread'in pwrite süresi kapasiteyi, bekleme süreleri de
     * sınırı belirleyen kaynağı gösterir:
     *  - yazıcı >= %85 meşgul  -> DİSK (UFS/FUSE) yazma sınırı
     *  - yazıcı >= %35 boşta    -> AĞ/Wi-Fi (veri gelmesini bekliyor)
     */
    private fun receiverDiagLine(
        elapsedNs: Long,
        writeNs: Long,
        writeBytes: Long,
        poolWaitNs: Long,
        idleNs: Long,
        readers: Int
    ): String {
        if (elapsedNs <= 0L || writeBytes <= 0L || writeNs <= 0L) return ""
        val capMbps = (writeBytes / (1024.0 * 1024.0)) / (writeNs / 1_000_000_000.0)
        val busy = (100.0 * writeNs / elapsedNs).coerceIn(0.0, 100.0)
        val idle = (100.0 * idleNs / elapsedNs).coerceIn(0.0, 100.0)
        val readerBlocked = if (readers > 0) {
            (100.0 * poolWaitNs / (elapsedNs.toDouble() * readers)).coerceIn(0.0, 100.0)
        } else {
            0.0
        }
        val verdict = when {
            busy >= 85.0 -> "Sınır: DİSK YAZMA"
            idle >= 35.0 -> "Sınır: AĞ/Wi-Fi"
            else -> "Dengeli"
        }
        return String.format(
            "Analiz (alıcı): %s • disk yazma kapasitesi %.0f MB/s • yazıcı %%%.0f meşgul / %%%.0f bekliyor • okuyucu tıkanma %%%.0f",
            verdict, capMbps, busy, idle, readerBlocked
        )
    }

    private fun describeIoFailure(t: Throwable?): String? {
        if (t == null) return null
        val msg = (t.message ?: t.toString())
        return if (msg.contains("ENOSPC") || msg.contains("No space left", ignoreCase = true)) {
            "Neden: depolama alanı doldu."
        } else {
            "Neden: disk yazma hatası (" + msg.take(80) + ")."
        }
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
        val finalizeLatch = CountDownLatch(1)
        receiverFinalizeLatch = finalizeLatch

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

            // Yükseltme politikası: (1) bağlantı zaten doğrudan değilse (hotspot/P2P) VE (2) toplam boyut başa baş
            // noktasını aşıyorsa VE (3) kullanıcı yüksek bant tercihini kapatmadıysa denenir. Aksi halde LAN/hotspot
            // üzerinden ANINDA başlanır (v12: her LAN transferinde 3-15 sn müzakere).
            val upgradeWorthIt = totalBatchBytes >= UPGRADE_MIN_TOTAL_BYTES
            val upgradeAllowed = !isAlreadyDirectLink && upgradeWorthIt && isHighBandUpgradeEnabled()

            val p2pGroupDeferred = CompletableDeferred<P2pGroupCredentials?>()
            if (upgradeAllowed && preferReceiverAsGo) {
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
            var phaseAOffered = false
            if (receiverP2pCreds != null &&
                upgradeAllowed &&
                receiverP2pCreds.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
                receiverP2pCreds.freqMhz >= 5000
            ) {
                phaseAOffered = true
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
            // v12: Faz A denenip başarısız olsa bile Faz B de denenirdi (ek ≈ 8 sn; Faz A'yı bozan eşzamanlılık/radyo
            // sorunu çoğunlukla Faz B'yi de bozar). Şimdi Faz B yalnızca Faz A HİÇ denenmediyse (alıcı GO olamadıysa veya
            // gönderici daha yetenekliyse) yapılır; Faz A başarısızsa doğrudan LAN'a düşülür.
            val allowSenderGoFallback = upgradeAllowed && !upgradedToP2p && !phaseAOffered
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

                // --- Ölçüm sayaçları (yalnızca gözlem; veri yolunu değiştirmez): darboğazın DİSK mi AĞ mı olduğunu gösterir ---
                val diagWriteNs = AtomicLong(0L)      // yazıcı thread'in pwrite içinde geçirdiği süre
                val diagWriteBytes = AtomicLong(0L)   // pwrite ile yazılan bayt
                val diagPoolWaitNs = AtomicLong(0L)   // reader'ların boş buffer beklediği süre (disk geri-basıncı)
                val diagWriterIdleNs = AtomicLong(0L) // yazıcının veri beklediği süre (ağ/gönderici sınırı)
                var receiveFailure: Throwable? = null
                val linkSampler = LinkSampler(context, senderIp)
                linkSampler.start()

                var raf: RandomAccessFile? = null
                var fileChannel: FileChannel? = null

                try {
                    raf = RandomAccessFile(targetFile, "rw")
                    val rawFd = raf.fd
                    fileChannel = raf.channel
                    fileChannel.position(0L)

                    if (useParallelStripes && workerChannels.isNotEmpty() && expectedFileSize > 0L) {
                        val totalStripes = ((expectedFileSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
                        // Havuz dosyanın ihtiyacına göre boyutlanır: 5 MB'lık dosya için 64 MB direct buffer
                        // tahsis edip sıfırlamak (ve çöpe atmak) toplu transferlerde saniyeler kaybettirir.
                        val poolSize = minOf(REORDER_RING_BUFFERS, totalStripes + workerChannels.size)
                        val spillThreshold = maxOf(4, poolSize / 2)
                        val freeBuffers = ArrayBlockingQueue<ByteBuffer>(poolSize)
                        for (i in 0 until poolSize) {
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
                                        val poolWaitStartNs = System.nanoTime()
                                        while (dataBuf == null && !sessionCancel.get() && !writerError.get()) {
                                            dataBuf = freeBuffers.poll(80L, TimeUnit.MILLISECONDS)
                                        }
                                        diagPoolWaitNs.addAndGet(System.nanoTime() - poolWaitStartNs)
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
                                            // putIfAbsent: aynı stripe iki kez gelirse (gönderici hatalı bir soketten yeniden
                                            // kuyruğa aldıysa) ikincisi havuzu SIZDIRMADAN geri dönüştürülür.
                                            val prev = completedStripes.putIfAbsent(stripeIdx, dataBuf)
                                            if (prev != null) {
                                                dataBuf.clear()
                                                freeBuffers.offer(dataBuf)
                                            } else {
                                                synchronized(reorderLock) {
                                                    reorderLock.notifyAll()
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

                        // Bir stripe'ı diske işler. writtenStripesBitSet, "stripe i diskte geçerli" bilgisinin TEK doğruluk
                        // kaynağıdır ve hiçbir koşulda (duraklat/devam dahil) geri alınmaz/temizlenmez.
                        fun commitStripe(idx: Int, buf: ByteBuffer) {
                            if (writtenStripesBitSet.get(idx)) {
                                buf.clear()
                                freeBuffers.offer(buf)
                                return
                            }
                            val expectLen = buf.remaining()
                            val t0 = System.nanoTime()
                            val w = writeBufferAtOffsetSingleThread(rawFd, fc, buf, idx.toLong() * STRIPE_BYTES.toLong())
                            diagWriteNs.addAndGet(System.nanoTime() - t0)
                            if (w != expectLen.toLong()) {
                                throw java.io.IOException("Kısa yazma: $w / $expectLen bayt (stripe $idx)")
                            }
                            diagWriteBytes.addAndGet(w)
                            totalBytesWrittenAtomic.addAndGet(w)
                            buf.clear()
                            freeBuffers.offer(buf)
                            writtenStripesBitSet.set(idx)
                            if (idx == nextContiguousStripe) {
                                nextContiguousStripe = writtenStripesBitSet.nextClearBit(nextContiguousStripe + 1)
                            }
                        }

                        // Dedicated Single UFS Writer Thread (Zero Multi-Thread FUSE Contention + Zero HoL Stall!)
                        var previewApplied = false
                        var previewWatermarkBytes = 0L
                        var wasPaused = false
                        var lastPausedUiMs = 0L
                        while (nextContiguousStripe < totalStripes && !sessionCancel.get() && !writerError.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
                                wasPaused = true
                                if (!previewApplied) {
                                    try {
                                        // DURAKLATMA MODU: yalnızca BİTİŞİK stripe'lar diske yazılır; sıra dışı gelenler bellekte
                                        // bekler. Böylece diskte watermark ötesinde veri kalmaz ve önizleme kuyruğunun
                                        // truncate'i hiçbir geçerli veriyi yok edemez (v12'de edebiliyordu).
                                        while (true) {
                                            val pend = completedStripes.remove(nextContiguousStripe) ?: break
                                            commitStripe(nextContiguousStripe, pend)
                                        }
                                        // Havuz tıkanmasın (reader'lar boş buffer bulamayıp delikteki stripe'ı okuyamaz):
                                        // kuyruk eşiği aşılırsa en düşük stripe diske yazılır (v12'nin Anti-HoL ilkesi).
                                        if (completedStripes.size >= spillThreshold) {
                                            val first = completedStripes.pollFirstEntry()
                                            if (first != null) commitStripe(first.key, first.value)
                                        }
                                        val contiguousNow = (nextContiguousStripe.toLong() * STRIPE_BYTES.toLong())
                                            .coerceAtMost(expectedFileSize)
                                        contiguousWatermarkBytes.set(contiguousNow)
                                        // Önizleme (moov kuyruğu) YALNIZCA watermark ötesinde diske yazılmış stripe yoksa uygulanır.
                                        if (writtenStripesBitSet.nextSetBit(nextContiguousStripe) < 0) {
                                            if (mp4Meta != null && contiguousNow > 0L && contiguousNow < expectedFileSize) {
                                                Mp4FastStartHelper.applyPartialMoovTail(fc, contiguousNow, mp4Meta)
                                            }
                                            previewApplied = true
                                            previewWatermarkBytes = contiguousNow
                                        }
                                    } catch (t: Throwable) {
                                        receiveFailure = t
                                        writerError.set(true)
                                    }
                                }
                                val nowPauseMs = System.currentTimeMillis()
                                if (nowPauseMs - lastPausedUiMs >= 120L) {
                                    lastPausedUiMs = nowPauseMs
                                    onProgressUpdate(
                                        TransferProgress(
                                            fileName = targetFile.name,
                                            fileIndex = fileIdx,
                                            totalFiles = fileCount,
                                            bytesTransferred = contiguousWatermarkBytes.get(),
                                            totalBytes = expectedFileSize,
                                            speedMbps = 0.0,
                                            etaSeconds = 0L,
                                            elapsedMillis = nowPauseMs - startWallMs,
                                            status = TransferStatus.PAUSED,
                                            transportLabel = transportLabel,
                                            isSending = false,
                                            savedFilePath = targetFile.absolutePath,
                                            previewReady = previewApplied,
                                            message = when {
                                                !previewApplied -> "Transfer Duraklatıldı (uçuştaki parçalar bekleniyor…)"
                                                mp4Meta != null -> "Transfer Duraklatıldı (Oynatmaya Hazır)"
                                                else -> "Transfer Duraklatıldı"
                                            }
                                        )
                                    )
                                }
                                if (writerError.get()) break
                                Thread.sleep(40L)
                            }
                            if (wasPaused && !sessionCancel.get()) {
                                wasPaused = false
                                speedSamples.clear()
                                // DEVAM: bitset ve sayaçlara DOKUNULMAZ. Yalnızca önizleme kuyruğu geri alınır.
                                // (v12 burada bitset.clear + totalBytesWritten sıfırlama + truncate yapıp sıra dışı geçerli
                                //  stripe'ları yok ediyor, watermark'ı sonsuza dek takılı bırakıyordu.)
                                if (previewApplied && mp4Meta != null) {
                                    Mp4FastStartHelper.restoreOriginalMdatForResume(fc, previewWatermarkBytes, mp4Meta)
                                }
                                previewApplied = false
                            }
                            if (writerError.get()) break
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
                                val idleStartNs = System.nanoTime()
                                synchronized(reorderLock) {
                                    if (completedStripes.isEmpty() && activeReaders.get() > 0 && !sessionCancel.get()) {
                                        reorderLock.wait(12L)
                                    }
                                }
                                diagWriterIdleNs.addAndGet(System.nanoTime() - idleStartNs)
                                continue
                            }

                            try {
                                commitStripe(targetStripeIdx, readyBuf)
                            } catch (t: Throwable) {
                                receiveFailure = t
                                writerError.set(true)
                                break
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
                        while (nextContiguousStripe < totalStripes && !writerError.get()) {
                            val pending = completedStripes.remove(nextContiguousStripe) ?: break
                            try {
                                commitStripe(nextContiguousStripe, pending)
                            } catch (t: Throwable) {
                                receiveFailure = t
                                break
                            }
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
                        var singlePreviewApplied = false
                        while (contiguousWatermarkBytes.get() < expectedFileSize && !sessionCancel.get()) {
                            var singleWasPaused = false
                            while (isPaused.get() && !sessionCancel.get()) {
                                singleWasPaused = true
                                // v12: önizleme kuyruğu (ve her seferinde fsync) duraklatma boyunca 90 ms'de bir YENİDEN yazılıyordu
                                // ve devam edilirken mdat başlığı hiç geri alınmıyordu. Şimdi: duraklatmada 1 kez, devamda geri al.
                                if (!singlePreviewApplied) {
                                    try {
                                        val contiguousNow = contiguousWatermarkBytes.get()
                                        if (mp4Meta != null && contiguousNow > 0L && contiguousNow < expectedFileSize) {
                                            Mp4FastStartHelper.applyPartialMoovTail(fileChannel, contiguousNow, mp4Meta)
                                        }
                                        singlePreviewApplied = true
                                    } catch (_: Throwable) {
                                    }
                                }
                                Thread.sleep(90L)
                            }
                            if (singleWasPaused && !sessionCancel.get() && singlePreviewApplied) {
                                singlePreviewApplied = false
                                if (mp4Meta != null) {
                                    Mp4FastStartHelper.restoreOriginalMdatForResume(
                                        fileChannel,
                                        contiguousWatermarkBytes.get(),
                                        mp4Meta
                                    )
                                }
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
                            val singleT0 = System.nanoTime()
                            val w = writeBufferAtOffsetSingleThread(rawFd, fileChannel, singleDirectBuf, offset)
                            diagWriteNs.addAndGet(System.nanoTime() - singleT0)
                            diagWriteBytes.addAndGet(w)
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
                } catch (t: Throwable) {
                    if (receiveFailure == null) receiveFailure = t
                } finally {
                    val contiguousBytes = contiguousWatermarkBytes.get().coerceAtMost(expectedFileSize)
                    val totalBytesReceived = if (totalBytesWrittenAtomic.get() >= expectedFileSize) {
                        expectedFileSize
                    } else {
                        contiguousBytes
                    }
                    val isPartial = totalBytesReceived in 1L until expectedFileSize
                    var onDiskSize = totalBytesReceived
                    try {
                        if (fileChannel != null && fileChannel.isOpen) {
                            if (isPartial && mp4Meta != null) {
                                onDiskSize = Mp4FastStartHelper.applyPartialMoovTail(fileChannel, totalBytesReceived, mp4Meta)
                            } else {
                                // fsync (force) KRİTİK YOLDA DEĞİL: sayfa önbelleği uygulama kapansa bile çekirdek tarafından
                                // yazılır; fsync yalnızca ani güç kaybına karşıdır ve büyük dosyalarda hem "Tamamlandı"
                                // durumunu hem de göndericiye giden onayı saniyelerce geciktirir.
                                fileChannel.truncate(totalBytesReceived)
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

                    // Gönderene DİSKTE KORUNAN bayt sayısı bildirilir (tamamlandıysa = beklenen boyut). Böylece kısmi/iptal
                    // durumunda sayı iki cihazda da aynı görünür (v12: gönderici kendi soket sayacını gösteriyordu).
                    try {
                        dos.writeLong(totalBytesReceived)
                        dos.flush()
                    } catch (_: Throwable) {
                    }

                    if (totalBytesReceived == 0L && expectedFileSize > 0L) {
                        // Hiç veri korunmadıysa 0 baytlık artık dosya bırakma.
                        try {
                            targetFile.delete()
                        } catch (_: Throwable) {
                        }
                    }

                    val elapsedNsAll = (System.nanoTime() - startTimeNs).coerceAtLeast(1L)
                    val diagLine = receiverDiagLine(
                        elapsedNs = elapsedNsAll,
                        writeNs = diagWriteNs.get(),
                        writeBytes = diagWriteBytes.get(),
                        poolWaitNs = diagPoolWaitNs.get(),
                        idleNs = diagWriterIdleNs.get(),
                        readers = workerChannels.size
                    )
                    val failureNote = describeIoFailure(receiveFailure)
                    linkSampler.stop()
                    val rxLinkLine = linkSampler.summary

                    if (totalBytesReceived > 0L) {
                        StorageHelper.notifyMediaScanner(context, targetFile)
                        val elapsedMs = (System.currentTimeMillis() - startWallMs).coerceAtLeast(1L)
                        val elapsedSec = (elapsedNsAll / 1_000_000_000.0).coerceAtLeast(0.001)
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

                        val finalMessage = buildString {
                            if (isPartial) {
                                append("Kısmi Dosya Korundu (%$pct • ${StorageHelper.formatFileSize(totalBytesReceived)} / ${StorageHelper.formatFileSize(expectedFileSize)})")
                                if (onDiskSize != totalBytesReceived) {
                                    append("\nDosya boyutu: ${StorageHelper.formatFileSize(onDiskSize)} (video önizleme kuyruğu dahil)")
                                }
                                if (failureNote != null) append("\n$failureNote")
                            } else {
                                append("Tamamlandı • Süre: ${formatDurationSec(elapsedMs)} • Ort. Hız: ${String.format("%.1f", avgSpeedMbps)} MB/s")
                            }
                            if (diagLine.isNotEmpty()) append("\n$diagLine")
                            if (rxLinkLine != null) append("\n$rxLinkLine")
                        }

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
                                message = finalMessage
                            )
                        )
                    } else if (expectedFileSize > 0L) {
                        onProgressUpdate(
                            TransferProgress(
                                fileName = targetFile.name,
                                fileIndex = fileIdx,
                                totalFiles = fileCount,
                                bytesTransferred = 0L,
                                totalBytes = expectedFileSize,
                                speedMbps = 0.0,
                                etaSeconds = 0L,
                                elapsedMillis = (System.currentTimeMillis() - startWallMs).coerceAtLeast(1L),
                                status = TransferStatus.PARTIAL_PRESERVED,
                                transportLabel = transportLabel,
                                isSending = false,
                                savedFilePath = null,
                                message = "Kısmi dosya oluşmadı (0 B kaydedildi)" + (failureNote?.let { "\n$it" } ?: "")
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
                // Kontrol kanalı FIN ile kapanır: az önce yazılan "korunan bayt" sayısı (8 bayt) RST ile çöpe gitmesin.
                closeChannelGracefully(clientChannel)
            } else {
                workers.forEach { closeChannelGracefully(it) }
                closeChannelGracefully(clientChannel)
            }
            if (receiverFinalizeLatch === finalizeLatch) receiverFinalizeLatch = null
            finalizeLatch.countDown()
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
        // 0 = bağlanma/el sıkışma, 1 = onay/müzakere, 2 = veri fazı. Hata mesajını doğru sınıflandırmak için.
        var phase = 0
        var currentSentCounter: AtomicLong? = null
        var currentFileSize = 0L
        var stopBatch = false

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
            phase = 1

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

            // 4. Open 6 Parallel Turbo Data SocketChannels CONCURRENTLY.
            // v12 bunları ARDIŞIK açıyordu: 6 × (bağlantı RTT'si [+ başarısızlıkta 8 deneme × 1 sn]). Wi-Fi güç tasarrufu /
            // hotspot'ta RTT'ler büyükken bu, kurulum süresini doğrudan katlıyordu. Paralel açılışta toplam ≈ 1 RTT.
            // Hedef, kontrol kanalının zaten ulaştığı IP ise (LAN/hotspot) uzun yeniden deneme gereksizdir.
            val dataRetryAttempts = if (activeDataTargetIp == hostIp) 3 else 8
            workerChannels.addAll(
                openDataChannelsParallel(activeDataTargetIp, port, sessionToken, sessionCancel, dataRetryAttempts)
            )

            if (workerChannels.isEmpty() && activeDataTargetIp != hostIp && !sessionCancel.get()) {
                activeDataTargetIp = hostIp
                transportLabel = resolveActiveBandLabel(initialTransportLabel)
                workerChannels.addAll(openDataChannelsParallel(hostIp, port, sessionToken, sessionCancel, 3))
            }

            activeSenderWorkerChannels.addAll(workerChannels)
            dos.writeInt(workerChannels.size)
            dos.flush()

            val confirmedWorkerCount = dis.readInt()
            val canUseParallelWorkers = confirmedWorkerCount > 0 && workerChannels.isNotEmpty()
            phase = 2

            val fallbackDirectBuffer = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            // 5. Send Each File
            for ((fileIdx, item) in items.withIndex()) {
                if (sessionCancel.get() || stopBatch) break

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
                    currentSentCounter = totalBytesSentAtomic
                    currentFileSize = actualSize
                    val startTimeNs = System.nanoTime()
                    val startWallMs = System.currentTimeMillis()
                    val lastUiUpdateMs = AtomicLong(0L)
                    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
                    // Ölçüm sayaçları (yalnızca gözlem): darboğazın DİSK OKUMA mı AĞ mı olduğunu gösterir.
                    val diagReadNs = AtomicLong(0L)
                    val diagSockNs = AtomicLong(0L)
                    var diagWorkers = 1
                    val txLinkSampler = LinkSampler(context, activeDataTargetIp)
                    txLinkSampler.start()

                    if (useParallelStripes && primaryHandle != null) {
                        val totalStripes = ((actualSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
                        val nextStripeIdx = AtomicInteger(0)
                        val activeChannels = workerChannels.take(confirmedWorkerCount)
                        diagWorkers = activeChannels.size.coerceAtLeast(1)
                        // Gönderilemeyen stripe'lar (okuma hatası / ölü soket) kaybolmaz: başka worker yeniden dener.
                        val requeue = ConcurrentLinkedQueue<Int>()
                        val inFlightStripes = AtomicInteger(0)
                        val readFailures = AtomicInteger(0) // ardışık başarısız okuma sayısı
                        val fatalReadError = AtomicBoolean(false)

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
                                        val gather = arrayOf(hdrBuf, workerDirectBuf)

                                        while (!sessionCancel.get() && !fatalReadError.get()) {
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

                                            // İş seçimi: önce yeniden kuyruğa alınanlar, sonra yeni stripe (iş-çalma, bariyer YOK).
                                            // inFlight, iş aranmadan ÖNCE artırılır: böylece "iş kalmadı" kararı verilirken başka bir
                                            // worker'ın yeniden kuyruğa alacağı stripe gözden kaçmaz.
                                            inFlightStripes.incrementAndGet()
                                            var sIdx = requeue.poll() ?: -1
                                            if (sIdx < 0) {
                                                val n = nextStripeIdx.getAndIncrement()
                                                if (n < totalStripes) sIdx = n else nextStripeIdx.set(totalStripes)
                                            }
                                            if (sIdx < 0) {
                                                inFlightStripes.decrementAndGet()
                                                if (inFlightStripes.get() == 0 && requeue.isEmpty()) {
                                                    hdrBuf.clear()
                                                    hdrBuf.putInt(-1)
                                                    hdrBuf.putInt(0)
                                                    hdrBuf.flip()
                                                    writeFullyToChannel(wChan, hdrBuf)
                                                    break
                                                }
                                                Thread.sleep(2L)
                                                continue
                                            }

                                            var stripeSent = false
                                            var socketDead = false
                                            try {
                                                val stripeOffset = sIdx.toLong() * STRIPE_BYTES.toLong()
                                                val stripeLen = (actualSize - stripeOffset)
                                                    .coerceAtMost(STRIPE_BYTES.toLong()).toInt()

                                                // 1) ÖNCE OKU (sayfa-hizalı 1 MB ofset, tampon konumu 0 = v12 ile aynı hizalama).
                                                //    v12 başlığı okumadan ÖNCE gönderiyordu: okuma başarısız olursa alıcı hiç gelmeyecek
                                                //    veriyi bekler, stripe kalıcı kaybolur ve alıcıda kalıcı bir delik oluşurdu.
                                                workerDirectBuf.clear()
                                                workerDirectBuf.limit(stripeLen)
                                                val readT0 = System.nanoTime()
                                                val readBytes = readBufferAtOffsetLockless(
                                                    fd = wHandle.fd,
                                                    fc = wHandle.channel,
                                                    buf = workerDirectBuf,
                                                    offset = stripeOffset
                                                )
                                                diagReadNs.addAndGet(System.nanoTime() - readT0)
                                                if (readBytes < stripeLen) {
                                                    // ARDIŞIK 8 başarısız okuma = kalıcı hata (dosya silindi / izin kalktı) -> oturumu bitir.
                                                    // Seyrek geçici hatalar yalnızca stripe'ı yeniden kuyruğa alır; aktarımı öldürmez.
                                                    if (readFailures.incrementAndGet() >= 8) fatalReadError.set(true)
                                                    Thread.sleep(20L)
                                                    continue
                                                }
                                                readFailures.set(0)
                                                workerDirectBuf.flip()

                                                // 2) Başlık + veri TEK gathering write (writev): ayrı 8 baytlık TCP segmenti ve ek
                                                //    syscall yok. Veri tamponu hâlâ konum 0'da (hizasız Os.pread DEĞİL).
                                                hdrBuf.clear()
                                                hdrBuf.putInt(sIdx)
                                                hdrBuf.putInt(stripeLen)
                                                hdrBuf.flip()
                                                val sockT0 = System.nanoTime()
                                                while ((hdrBuf.hasRemaining() || workerDirectBuf.hasRemaining()) && !sessionCancel.get()) {
                                                    val w = wChan.write(gather)
                                                    if (w < 0L) {
                                                        socketDead = true
                                                        break
                                                    }
                                                }
                                                diagSockNs.addAndGet(System.nanoTime() - sockT0)
                                                if (socketDead || hdrBuf.hasRemaining() || workerDirectBuf.hasRemaining()) {
                                                    socketDead = true
                                                } else {
                                                    stripeSent = true
                                                    totalBytesSentAtomic.addAndGet(stripeLen.toLong())
                                                }
                                            } catch (_: Throwable) {
                                                socketDead = true
                                            } finally {
                                                // Önce yeniden kuyruğa ekle, SONRA inFlight'ı azalt (sıra, bitiş kararı için kritik).
                                                if (!stripeSent) requeue.add(sIdx)
                                                inFlightStripes.decrementAndGet()
                                            }
                                            if (socketDead) break

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

                    val localBytesSent = totalBytesSentAtomic.get().coerceAtMost(actualSize)
                    var bytesSent = localBytesSent
                    var receiverConfirmed = false
                    val locallyCancelled = sessionCancel.get()
                    try {
                        if (!locallyCancelled && localBytesSent == actualSize) {
                            dos.flush()
                            controlChannel.socket().soTimeout = 30_000
                        } else {
                            // Kısmi sonuç (iptal / kopma): alıcının DİSKTE KORUDUĞU bayt sayısını kısa süre bekle. Alıcı bu
                            // sayıyı her durumda gönderir; böylece iki cihaz AYNI kısmi boyutu gösterir. (v12: gönderici kendi
                            // soket sayacını — çekirdek tamponundaki ~100 MB dahil — "gönderildi" diye gösteriyordu.)
                            controlChannel.socket().soTimeout = 2500
                        }
                        val reported = dis.readLong()
                        if (reported >= 0L) {
                            bytesSent = reported.coerceAtMost(actualSize)
                            receiverConfirmed = true
                        }
                    } catch (_: Throwable) {
                    }

                    val elapsedMs = (System.currentTimeMillis() - startWallMs).coerceAtLeast(1L)
                    val totalElapsedNs = (System.nanoTime() - startTimeNs).coerceAtLeast(1L)
                    val totalElapsedSec = (totalElapsedNs / 1_000_000_000.0).coerceAtLeast(0.001)
                    val finalSpeedMbps = (bytesSent / (1024.0 * 1024.0)) / totalElapsedSec
                    val isPartial = bytesSent < actualSize
                    val pct = if (actualSize > 0L) (bytesSent * 100L) / actualSize else 100L
                    val diagLine = senderDiagLine(
                        elapsedNs = totalElapsedNs,
                        readNs = diagReadNs.get(),
                        sockNs = diagSockNs.get(),
                        workers = diagWorkers,
                        bytes = localBytesSent
                    )
                    txLinkSampler.stop()
                    val txLinkLine = txLinkSampler.summary

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
                            message = buildString {
                                if (isPartial) {
                                    if (receiverConfirmed) {
                                        append("Durduruldu • Alıcıda korunan: ${StorageHelper.formatFileSize(bytesSent)} / ${StorageHelper.formatFileSize(actualSize)} (%$pct)")
                                    } else {
                                        append("Durduruldu • Ağa verilen: ${StorageHelper.formatFileSize(localBytesSent)} / ${StorageHelper.formatFileSize(actualSize)}")
                                        append("\nAlıcıdaki kesin kısmi dosya boyutu alıcı cihazda görünür.")
                                    }
                                } else {
                                    append("Tamamlandı • Süre: ${formatDurationSec(elapsedMs)} • Ort. Hız: ${String.format("%.1f", finalSpeedMbps)} MB/s")
                                }
                                if (diagLine.isNotEmpty()) append("\n$diagLine")
                                if (txLinkLine != null) append("\n$txLinkLine")
                            }
                        )
                    )
                    // Alıcı bu dosyayı kısmi bıraktıysa (iptal/kopma) toplu gönderimde sonraki dosyalara geçilmez:
                    // v12'de bu durumda ölü kanala yazılıp yanıltıcı bir "ulaşılamadı" hatası üretiliyordu.
                    if (isPartial) stopBatch = true
                } finally {
                    primaryHandle?.closeAll()
                }
            }
        } catch (t: Throwable) {
            val firstSize = items.firstOrNull()?.sizeBytes ?: 0L
            val reportTotal = if (currentFileSize > 0L) currentFileSize else firstSize
            val sentSoFar = (currentSentCounter?.get() ?: 0L).coerceAtMost(reportTotal)
            val reportName = items.firstOrNull()?.name ?: "Dosya"
            if (sessionCancel.get()) {
                // v12: her iptalde "Kısmi dosya korundu" + 0 bayt gösteriliyordu (veri fazı başlamamış olsa bile).
                onProgressUpdate(
                    TransferProgress(
                        fileName = reportName,
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = if (phase >= 2) sentSoFar else 0L,
                        totalBytes = reportTotal,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.PARTIAL_PRESERVED,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = if (phase >= 2) {
                            "Durduruldu • Ağa verilen: ${StorageHelper.formatFileSize(sentSoFar)}\nAlıcıdaki kesin kısmi dosya boyutu alıcı cihazda görünür."
                        } else {
                            "Aktarım başlamadan iptal edildi (kaydedilen veri yok)."
                        }
                    )
                )
            } else if (phase >= 2) {
                // Aktarım ORTASINDA kopma bir "bağlantı kurulamadı" hatası DEĞİLDİR: alıcıdaki kısmi dosya korunur.
                onProgressUpdate(
                    TransferProgress(
                        fileName = reportName,
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = sentSoFar,
                        totalBytes = reportTotal,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.PARTIAL_PRESERVED,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = "Bağlantı koptu • Ağa verilen: ${StorageHelper.formatFileSize(sentSoFar)} / ${StorageHelper.formatFileSize(reportTotal)}\nAlıcı cihazdaki kısmi dosya korunur (kesin boyut alıcıda görünür)."
                    )
                )
            } else {
                onProgressUpdate(
                    TransferProgress(
                        fileName = reportName,
                        fileIndex = 0,
                        totalFiles = items.size,
                        bytesTransferred = 0L,
                        totalBytes = firstSize,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        elapsedMillis = 0L,
                        status = TransferStatus.ERROR,
                        transportLabel = transportLabel,
                        isSending = true,
                        message = describeConnectFailure(t, hostIp)
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

    /**
     * 6 veri soketini EŞZAMANLI açar (toplam ≈ 1 RTT). Hedef zaten ulaşıldığı bilinen bir IP ise (LAN/hotspot) az sayıda
     * deneme yeterlidir; yeni kurulmuş P2P grubunda ARP/rota henüz hazır olmayabileceği için daha çok deneme yapılır.
     */
    private fun openDataChannelsParallel(
        ip: String,
        port: Int,
        sessionToken: Long,
        cancel: AtomicBoolean,
        maxAttempts: Int
    ): List<SocketChannel> {
        val results = arrayOfNulls<SocketChannel>(PARALLEL_DATA_STREAMS)
        val threads = (0 until PARALLEL_DATA_STREAMS).map { i ->
            Thread {
                for (attempt in 0 until maxAttempts) {
                    if (cancel.get()) break
                    var candidate: SocketChannel? = null
                    try {
                        candidate = SocketChannel.open()
                        candidate.configureBlocking(true)
                        configureChannelSocketBuffers(candidate, isSender = true)
                        CompatibilityHelper.bindSocketToOptimalInterface(context, candidate.socket(), ip)
                        candidate.socket().connect(InetSocketAddress(ip, port), 1000)
                        val turboOut = DataOutputStream(candidate.socket().getOutputStream())
                        turboOut.writeInt(PROTOCOL_MAGIC_TURBO_DATA)
                        turboOut.writeLong(sessionToken)
                        turboOut.flush()
                        results[i] = candidate
                        break
                    } catch (_: Throwable) {
                        // Başarısız adayı kapat (v12 sızdırıyordu)
                        try {
                            candidate?.close()
                        } catch (_: Throwable) {
                        }
                        try {
                            Thread.sleep(90L)
                        } catch (_: Throwable) {
                        }
                    }
                }
            }.apply {
                name = "Zeph-Connect-$i"
                start()
            }
        }
        for (t in threads) {
            try {
                t.join()
            } catch (_: Throwable) {
            }
        }
        return results.filterNotNull()
    }

    /**
     * Gönderici tarafı darboğaz analizi (yalnızca gözlem): her worker thread'in zamanı "disk okuma" ve "soketi yazma"
     * arasında bölünür. Soket yazması çoğunlukla bloklanıyorsa sınır ağdır; okuma baskınsa sınır depolamadır.
     */
    private fun senderDiagLine(elapsedNs: Long, readNs: Long, sockNs: Long, workers: Int, bytes: Long): String {
        if (elapsedNs <= 0L || workers <= 0 || bytes <= 0L || readNs <= 0L) return ""
        val perThreadReadMbps = (bytes / (1024.0 * 1024.0)) / (readNs / 1_000_000_000.0)
        val denom = elapsedNs.toDouble() * workers
        val readBusy = (100.0 * readNs / denom).coerceIn(0.0, 100.0)
        val sockBlocked = (100.0 * sockNs / denom).coerceIn(0.0, 100.0)
        val verdict = when {
            sockBlocked >= 55.0 -> "Sınır: AĞ/Wi-Fi"
            readBusy >= 55.0 -> "Sınır: DİSK OKUMA"
            else -> "Dengeli"
        }
        return String.format(
            "Analiz (gönderici): %s • disk okuma %.0f MB/s (thread başına) • okuma %%%.0f / soket %%%.0f meşgul",
            verdict, perThreadReadMbps, readBusy, sockBlocked
        )
    }

    /**
     * Veri fazı sırasında Wi-Fi bağlantısını birkaç kez örnekler (transfer bittikten sonra sürücü hız adaptasyonunu
     * düşürmüş olabileceğinden bitişte bakmak yanıltıcıdır) ve EN YÜKSEK hızlı özeti döndürür.
     */
    private class LinkSampler(private val context: Context, private val peerIp: String) {
        @Volatile
        var summary: String? = null
        private var bestScore = -1
        private val thread = Thread {
            try {
                for (delayMs in longArrayOf(2000L, 3000L, 5000L)) {
                    Thread.sleep(delayMs)
                    val line = CompatibilityHelper.describeStaLinkToPeer(context, peerIp) ?: continue
                    val score = Regex("Tx (\\d+) / Rx (\\d+)").find(line)?.let {
                        maxOf(it.groupValues[1].toInt(), it.groupValues[2].toInt())
                    } ?: 0
                    if (score >= bestScore) {
                        bestScore = score
                        summary = line
                    }
                }
            } catch (_: Throwable) {
            }
        }.apply {
            name = "Zeph-LinkSampler"
            isDaemon = true
        }

        fun start() = thread.start()
        fun stop() {
            try {
                thread.interrupt()
            } catch (_: Throwable) {
            }
        }
    }

    private fun describeConnectFailure(t: Throwable, hostIp: String): String {
        val msg = (t.message ?: "")
        val reason = when {
            t is java.net.SocketTimeoutException || msg.contains("ETIMEDOUT") ->
                "zaman aşımı (cihaz yanıt vermedi; farklı ağda olabilirsiniz ya da ağ/hotspot istemci yalıtımı var)"
            t is java.net.NoRouteToHostException || msg.contains("ENETUNREACH") || msg.contains("EHOSTUNREACH") ->
                "ağ yolu yok (Wi-Fi/mobil veri geçişi veya farklı ağ)"
            t is java.net.ConnectException || msg.contains("ECONNREFUSED") ->
                "bağlantı reddedildi (karşı cihazda uygulama açık değil)"
            t is java.io.EOFException ->
                "karşı cihaz bağlantıyı erken kapattı"
            else -> t.javaClass.simpleName
        }
        return "Hedef cihaza ($hostIp) bağlanılamadı: $reason."
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
        // Alıcı dosyası sonlandırılırken (truncate / video önizleme kuyruğu / ack) süreç ölürse kısmi dosya yarım kalır.
        // "Durdur + uygulamayı kapat" akışında bu bekleme kısmi dosyanın tutarlı kalmasını sağlar. stop() ana
        // thread'den (onDestroy) çağrıldığı için bekleme SINIRLIDIR.
        try {
            receiverFinalizeLatch?.await(1500L, TimeUnit.MILLISECONDS)
        } catch (_: Throwable) {
        }
        releaseInProcessHardwareLocks()
        activeTransferJob?.cancel()
        serverJob?.cancel()
        httpServerJob?.cancel()
    }
}
