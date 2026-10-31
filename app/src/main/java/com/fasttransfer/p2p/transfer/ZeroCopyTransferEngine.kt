package com.fasttransfer.p2p.transfer

import android.content.Context
import android.os.Build
import android.os.ParcelFileDescriptor
import android.os.Process
import android.system.Os
import com.fasttransfer.p2p.model.IncomingTransferRequest
import com.fasttransfer.p2p.model.TransferHistoryRecord
import com.fasttransfer.p2p.model.TransferItem
import com.fasttransfer.p2p.model.TransferProgress
import com.fasttransfer.p2p.model.TransferStatus
import com.fasttransfer.p2p.util.StorageHelper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * KERNEL ZERO-COPY, LOCALSEND CONSENT HANDSHAKE & %5 PARTIAL FILE PRESERVATION ENGINE:
 * - 16 MB Socket Send/Receive Buffers (SO_SNDBUF = 16MB, SO_RCVBUF = 16MB) + 32 MB Kernel DMA transferTo chunks.
 * - Pre-transfer Consent Handshake: Receiver sees "Kabul Et / Reddet" dialog before a single file byte is written.
 * - Instant Stop/Cancel/Dismiss: Immediately closes the active SocketChannel so blocked kernel I/O wakes up in <1ms,
 *   seals any partial file (truncate + fsync + MediaScanner), and resets cleanly so the next transfer starts immediately.
 */
class ZeroCopyTransferEngine(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onIncomingConsentRequest: (IncomingTransferRequest) -> Unit,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit
) {
    companion object {
        const val SOCKET_BUFFER_BYTES = 16 * 1024 * 1024 // 16 MB TCP Window for Wi-Fi 6 / 5 GHz saturation
        const val DIRECT_BUFFER_BYTES = 8 * 1024 * 1024 // 8 MB Direct Off-Heap DMA Buffer
        const val ZERO_COPY_CHUNK_BYTES = 32L * 1024L * 1024L // 32 MB per kernel sendfile64/transferTo syscall
        private const val PROTOCOL_MAGIC = 0x5A455048 // "ZEPH"
        private const val DECISION_ACCEPTED: Byte = 1
        private const val DECISION_REJECTED: Byte = 2
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverJob: Job? = null
    private var httpServerJob: Job? = null
    private var activeTransferJob: Job? = null

    private val isPaused = AtomicBoolean(false)
    private val isCancelled = AtomicBoolean(false)

    @Volatile
    var autoAcceptIncoming: Boolean = false

    @Volatile
    private var activeSenderChannel: SocketChannel? = null

    @Volatile
    private var activeReceiverChannel: SocketChannel? = null

    @Volatile
    private var activeConsentLatch: CountDownLatch? = null

    val historyRecords: MutableList<TransferHistoryRecord> =
        Collections.synchronizedList(mutableListOf())

    @Volatile
    private var sharedLinkItems: List<TransferItem> = emptyList()

    fun startServer() {
        startZeroCopyReceiverServer()
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

    /**
     * Immediately interrupts any blocked socket read/write or consent handshake in <1ms,
     * preserving and sealing any partial file received so far.
     */
    fun cancelAndPreservePartialFile() {
        isCancelled.set(true)
        isPaused.set(false)
        activeConsentLatch?.countDown()
        scope.launch {
            try {
                activeSenderChannel?.close()
            } catch (_: Throwable) {
            }
            try {
                activeReceiverChannel?.close()
            } catch (_: Throwable) {
            }
        }
    }

    private fun configureChannelSocketBuffers(channel: SocketChannel) {
        try {
            val socket = channel.socket()
            socket.tcpNoDelay = true
            socket.keepAlive = true
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            socket.receiveBufferSize = SOCKET_BUFFER_BYTES
            socket.trafficClass = 0x08 // IPTOS_THROUGHPUT (Maximize MB/s bulk data rate)
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

    private fun startZeroCopyReceiverServer() {
        serverJob?.cancel()
        serverJob = scope.launch {
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
                    launch {
                        handleIncomingClientChannel(clientChannel)
                    }
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    serverChannel?.close()
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun handleIncomingClientChannel(clientChannel: SocketChannel) {
        activeReceiverChannel = clientChannel
        isCancelled.set(false)
        isPaused.set(false)
        try {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            clientChannel.configureBlocking(true)
            configureChannelSocketBuffers(clientChannel)

            val socket = clientChannel.socket()
            val senderIp = socket.inetAddress?.hostAddress ?: "Bilinmeyen IP"
            val dis = DataInputStream(socket.getInputStream())
            val dos = DataOutputStream(socket.getOutputStream())

            val magic = dis.readInt()
            if (magic != PROTOCOL_MAGIC) {
                clientChannel.close()
                return
            }

            // 1. Read Pre-Transfer Consent Handshake Header
            val senderAlias = dis.readUTF()
            val transportLabel = dis.readUTF().ifBlank { "Zero-Copy P2P" }
            val fileCount = dis.readInt()
            val totalBatchBytes = dis.readLong()
            val previewCount = dis.readInt()
            val previewLines = mutableListOf<String>()
            for (i in 0 until previewCount) {
                previewLines.add(dis.readUTF())
            }

            // 2. Ask Receiver User for Permission (unless Auto-Accept is enabled in Settings)
            var accepted = autoAcceptIncoming
            if (!accepted) {
                val latch = CountDownLatch(1)
                activeConsentLatch = latch
                var userDecision = false

                val req = IncomingTransferRequest(
                    senderAlias = senderAlias,
                    senderIp = senderIp,
                    transportLabel = transportLabel,
                    fileCount = fileCount,
                    totalBytes = totalBatchBytes,
                    filePreviewLines = previewLines.joinToString("\n"),
                    onDecision = { approved ->
                        userDecision = approved
                        latch.countDown()
                    }
                )
                onIncomingConsentRequest(req)

                // Wait up to 60 seconds for user to tap "Kabul Et" or "Reddet"
                latch.await(60, TimeUnit.SECONDS)
                activeConsentLatch = null
                accepted = userDecision && !isCancelled.get()
            }

            if (!accepted) {
                try {
                    dos.writeByte(DECISION_REJECTED.toInt())
                    dos.flush()
                } catch (_: Throwable) {
                }
                clientChannel.close()
                return
            }

            // Send ACCEPTED signal to Sender so high-speed streaming begins immediately
            dos.writeByte(DECISION_ACCEPTED.toInt())
            dos.flush()

            val directBuf = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            for (fileIdx in 0 until fileCount) {
                if (isCancelled.get()) break

                val fileName = dis.readUTF()
                val expectedFileSize = dis.readLong()

                val targetFile: File = StorageHelper.createDirectDownloadTarget(context, fileName)
                var totalBytesReceived = 0L
                val startTimeNs = System.nanoTime()
                var lastUiUpdateMs = 0L

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

                    while (totalBytesReceived < expectedFileSize && !isCancelled.get()) {
                        while (isPaused.get() && !isCancelled.get()) {
                            onProgressUpdate(
                                TransferProgress(
                                    fileName = targetFile.name,
                                    bytesTransferred = totalBytesReceived,
                                    totalBytes = expectedFileSize,
                                    speedMbps = 0.0,
                                    etaSeconds = 0L,
                                    status = TransferStatus.PAUSED,
                                    transportLabel = transportLabel,
                                    savedFilePath = targetFile.absolutePath,
                                    message = "Transfer Duraklatıldı"
                                )
                            )
                            Thread.sleep(100L)
                        }
                        if (isCancelled.get()) break

                        directBuf.clear()
                        val remaining = expectedFileSize - totalBytesReceived
                        if (remaining < directBuf.capacity()) {
                            directBuf.limit(remaining.toInt())
                        }

                        val bytesRead = clientChannel.read(directBuf)
                        if (bytesRead <= 0) {
                            break
                        }

                        directBuf.flip()
                        while (directBuf.hasRemaining()) {
                            totalBytesReceived += fileChannel.write(directBuf)
                        }

                        val nowMs = System.currentTimeMillis()
                        if (nowMs - lastUiUpdateMs >= 180L || totalBytesReceived == expectedFileSize) {
                            lastUiUpdateMs = nowMs
                            val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                            val speedMbps = (totalBytesReceived / (1024.0 * 1024.0)) / elapsedSec
                            val remainingBytes = (expectedFileSize - totalBytesReceived).coerceAtLeast(0L)
                            val etaSec = if (speedMbps > 0.01) {
                                ((remainingBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                            } else {
                                0L
                            }

                            onProgressUpdate(
                                TransferProgress(
                                    fileName = targetFile.name,
                                    bytesTransferred = totalBytesReceived,
                                    totalBytes = expectedFileSize,
                                    speedMbps = speedMbps,
                                    etaSeconds = etaSec,
                                    status = TransferStatus.TRANSFERRING,
                                    transportLabel = transportLabel,
                                    savedFilePath = targetFile.absolutePath
                                )
                            )
                        }
                    }
                } catch (_: Throwable) {
                    // Socket closed on cancel or connection drop -> proceed to finally block to seal partial file
                } finally {
                    // CRITICAL %5 PARTIAL FILE PRESERVATION & SEALING RULE:
                    // 1) Never call hedef dosya silme metodu!
                    // 2) Truncate fileChannel to totalBytesReceived so unwritten fallocate zeros are removed.
                    // 3) Flush kernel buffers to UFS/eMMC via force(true) and trigger MediaScanner.
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

                    if (totalBytesReceived > 0L) {
                        StorageHelper.notifyMediaScanner(context, targetFile)
                        val isPartial = totalBytesReceived < expectedFileSize
                        val record = TransferHistoryRecord(
                            fileName = targetFile.name,
                            filePath = targetFile.absolutePath,
                            bytesWritten = totalBytesReceived,
                            totalExpectedBytes = expectedFileSize,
                            isPartial = isPartial,
                            timestampMs = System.currentTimeMillis()
                        )
                        historyRecords.add(0, record)
                        onHistoryAdded(record)

                        val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                        val avgSpeedMbps = (totalBytesReceived / (1024.0 * 1024.0)) / elapsedSec
                        val pct = if (expectedFileSize > 0L) (totalBytesReceived * 100L) / expectedFileSize else 100L

                        onProgressUpdate(
                            TransferProgress(
                                fileName = targetFile.name,
                                bytesTransferred = totalBytesReceived,
                                totalBytes = expectedFileSize,
                                speedMbps = avgSpeedMbps,
                                etaSeconds = 0L,
                                status = if (isPartial) TransferStatus.PARTIAL_PRESERVED else TransferStatus.COMPLETED,
                                transportLabel = transportLabel,
                                savedFilePath = targetFile.absolutePath,
                                message = if (isPartial) {
                                    "Kısmi Dosya Korundu (%$pct • ${StorageHelper.formatFileSize(totalBytesReceived)}) - Anında Açılabilir!"
                                } else {
                                    "Tamamlandı (${StorageHelper.formatFileSize(totalBytesReceived)})"
                                }
                            )
                        )
                    }
                }
            }
        } catch (_: Throwable) {
        } finally {
            if (activeReceiverChannel === clientChannel) {
                activeReceiverChannel = null
            }
            try {
                clientChannel.close()
            } catch (_: Throwable) {
            }
        }
    }

    /**
     * SENDER ENGINE:
     * Performs LocalSend-style Consent Handshake first, then streams via Kernel Zero-Copy (transferTo).
     */
    fun sendItemsToPeers(
        items: List<TransferItem>,
        targetHostIps: List<Pair<String, Int>>,
        transportLabel: String
    ) {
        if (items.isEmpty() || targetHostIps.isEmpty()) return

        // Cleanly close any previous lingering socket before starting a new transfer
        try {
            activeSenderChannel?.close()
        } catch (_: Throwable) {
        }
        activeTransferJob?.cancel()

        isCancelled.set(false)
        isPaused.set(false)

        activeTransferJob = scope.launch {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            for ((hostIp, port) in targetHostIps) {
                if (isCancelled.get()) break
                sendItemsToSinglePeer(items, hostIp, port, transportLabel)
            }
        }
    }

    private fun sendItemsToSinglePeer(
        items: List<TransferItem>,
        hostIp: String,
        port: Int,
        transportLabel: String
    ) {
        var socketChannel: SocketChannel? = null
        try {
            val totalBatchBytes = items.sumOf { it.sizeBytes }
            onProgressUpdate(
                TransferProgress(
                    fileName = items.first().name,
                    bytesTransferred = 0L,
                    totalBytes = totalBatchBytes,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    status = TransferStatus.CONNECTING,
                    transportLabel = transportLabel,
                    message = "$hostIp:$port ile bağlantı kuruluyor..."
                )
            )

            socketChannel = SocketChannel.open()
            activeSenderChannel = socketChannel
            socketChannel.configureBlocking(true)
            configureChannelSocketBuffers(socketChannel)
            socketChannel.socket().connect(InetSocketAddress(hostIp, port), 5000)

            val dos = DataOutputStream(socketChannel.socket().getOutputStream())
            val dis = DataInputStream(socketChannel.socket().getInputStream())

            // 1. Send Handshake Header & Wait for Receiver's Approval ("Kabul Et" / "Reddet")
            dos.writeInt(PROTOCOL_MAGIC)
            dos.writeUTF(getLocalAlias())
            dos.writeUTF(transportLabel)
            dos.writeInt(items.size)
            dos.writeLong(totalBatchBytes)

            val previewItems = items.take(8)
            dos.writeInt(previewItems.size)
            for (pItem in previewItems) {
                dos.writeUTF("• ${pItem.name} (${StorageHelper.formatFileSize(pItem.sizeBytes)})")
            }
            dos.flush()

            onProgressUpdate(
                TransferProgress(
                    fileName = items.first().name,
                    bytesTransferred = 0L,
                    totalBytes = totalBatchBytes,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    status = TransferStatus.WAITING_APPROVAL,
                    transportLabel = transportLabel,
                    message = "Karşı cihazın ($hostIp) 'Kabul Et' onayı bekleniyor..."
                )
            )

            val decision = dis.readByte()
            if (decision != DECISION_ACCEPTED) {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.first().name,
                        bytesTransferred = 0L,
                        totalBytes = totalBatchBytes,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        status = TransferStatus.REJECTED,
                        transportLabel = transportLabel,
                        message = "Karşı cihaz dosya gönderimini reddetti."
                    )
                )
                return
            }

            val fallbackDirectBuffer = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            for (item in items) {
                if (isCancelled.get()) break

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

                    dos.writeUTF(item.name)
                    dos.writeLong(actualSize)
                    dos.flush()

                    var bytesSent = 0L
                    val startTimeNs = System.nanoTime()
                    var lastUiUpdateMs = 0L

                    if (item.inlineTextBytes != null) {
                        val buf = ByteBuffer.wrap(item.inlineTextBytes)
                        while (buf.hasRemaining() && !isCancelled.get()) {
                            bytesSent += socketChannel.write(buf)
                        }
                    } else if (fileChannel != null) {
                        var zeroCopySupported = true
                        while (bytesSent < actualSize && !isCancelled.get()) {
                            while (isPaused.get() && !isCancelled.get()) {
                                onProgressUpdate(
                                    TransferProgress(
                                        fileName = item.name,
                                        bytesTransferred = bytesSent,
                                        totalBytes = actualSize,
                                        speedMbps = 0.0,
                                        etaSeconds = 0L,
                                        status = TransferStatus.PAUSED,
                                        transportLabel = transportLabel,
                                        message = "Gönderim Duraklatıldı"
                                    )
                                )
                                Thread.sleep(100L)
                            }
                            if (isCancelled.get()) break

                            if (zeroCopySupported) {
                                try {
                                    val count = (actualSize - bytesSent).coerceAtMost(ZERO_COPY_CHUNK_BYTES)
                                    val transferred = fileChannel.transferTo(bytesSent, count, socketChannel)
                                    if (transferred > 0L) {
                                        bytesSent += transferred
                                    } else {
                                        zeroCopySupported = false
                                    }
                                } catch (e: Throwable) {
                                    if (isCancelled.get()) break
                                    zeroCopySupported = false
                                }
                            }

                            if (!zeroCopySupported && !isCancelled.get()) {
                                fallbackDirectBuffer.clear()
                                val rem = actualSize - bytesSent
                                if (rem < fallbackDirectBuffer.capacity()) {
                                    fallbackDirectBuffer.limit(rem.toInt())
                                }
                                val readCount = fileChannel.read(fallbackDirectBuffer, bytesSent)
                                if (readCount <= 0) break
                                fallbackDirectBuffer.flip()
                                while (fallbackDirectBuffer.hasRemaining() && !isCancelled.get()) {
                                    val written = socketChannel.write(fallbackDirectBuffer)
                                    if (written <= 0) break
                                    bytesSent += written
                                }
                            }

                            val nowMs = System.currentTimeMillis()
                            if (nowMs - lastUiUpdateMs >= 180L || bytesSent == actualSize) {
                                lastUiUpdateMs = nowMs
                                val elapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                                val speedMbps = (bytesSent / (1024.0 * 1024.0)) / elapsedSec
                                val remBytes = (actualSize - bytesSent).coerceAtLeast(0L)
                                val etaSec = if (speedMbps > 0.01) {
                                    ((remBytes / (1024.0 * 1024.0)) / speedMbps).toLong()
                                } else {
                                    0L
                                }
                                onProgressUpdate(
                                    TransferProgress(
                                        fileName = item.name,
                                        bytesTransferred = bytesSent,
                                        totalBytes = actualSize,
                                        speedMbps = speedMbps,
                                        etaSeconds = etaSec,
                                        status = TransferStatus.TRANSFERRING,
                                        transportLabel = transportLabel
                                    )
                                )
                            }
                        }
                    }

                    val totalElapsedSec = ((System.nanoTime() - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.001)
                    val finalSpeedMbps = (bytesSent / (1024.0 * 1024.0)) / totalElapsedSec
                    val isPartial = bytesSent < actualSize
                    val pct = if (actualSize > 0L) (bytesSent * 100L) / actualSize else 100L

                    onProgressUpdate(
                        TransferProgress(
                            fileName = item.name,
                            bytesTransferred = bytesSent,
                            totalBytes = actualSize,
                            speedMbps = finalSpeedMbps,
                            etaSeconds = 0L,
                            status = if (isPartial) TransferStatus.PARTIAL_PRESERVED else TransferStatus.COMPLETED,
                            transportLabel = transportLabel,
                            message = if (isPartial) {
                                "Durduruldu (%$pct gönderildi • Alıcıda kısmi dosya korundu)"
                            } else {
                                "Gönderim Tamamlandı (${StorageHelper.formatFileSize(bytesSent)})"
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
            if (isCancelled.get()) {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.firstOrNull()?.name ?: "Dosya",
                        bytesTransferred = 0L,
                        totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        status = TransferStatus.PARTIAL_PRESERVED,
                        transportLabel = transportLabel,
                        message = "İşlem kullanıcı tarafından durduruldu (Kısmi dosya korundu)."
                    )
                )
            } else {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.firstOrNull()?.name ?: "Dosya",
                        bytesTransferred = 0L,
                        totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        status = TransferStatus.ERROR,
                        transportLabel = transportLabel,
                        message = "Bağlantı sonlandı veya karşı cihaz kapattı."
                    )
                )
            }
        } finally {
            if (activeSenderChannel === socketChannel) {
                activeSenderChannel = null
            }
            try {
                socketChannel?.close()
            } catch (_: Throwable) {
            }
        }
    }

    private fun startHttpBrowserShareServer() {
        httpServerJob?.cancel()
        httpServerJob = scope.launch {
            var serverSocket: ServerSocket? = null
            try {
                serverSocket = ServerSocket(httpLinkPort)
                while (isActive) {
                    val client = serverSocket.accept() ?: continue
                    launch {
                        handleHttpClient(client)
                    }
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    serverSocket?.close()
                } catch (_: Throwable) {
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
        activeTransferJob?.cancel()
        serverJob?.cancel()
        httpServerJob?.cancel()
    }
}
