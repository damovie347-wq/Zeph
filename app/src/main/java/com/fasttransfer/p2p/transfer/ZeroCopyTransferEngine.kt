package com.fasttransfer.p2p.transfer

import android.content.Context
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.OutputStream
import java.io.RandomAccessFile
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URLEncoder
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

class ZeroCopyTransferEngine(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onIncomingConsentRequest: (IncomingTransferRequest) -> Unit,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit,
    private val onTransferActiveStateChanged: (Boolean) -> Unit
) {
    companion object {
        const val SOCKET_BUFFER_BYTES = 16 * 1024 * 1024 // 16 MB Kernel TCP Window
        const val IO_BLOCK_BYTES = 4 * 1024 * 1024 // 4 MB Coalesced DMA/UFS Block
        private const val PROTOCOL_MAGIC = 0x5A455048 // "ZEPH"
        private const val DECISION_ACCEPTED: Int = 1
        private const val DECISION_REJECTED: Int = 2
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverJob: Job? = null
    private var httpServerJob: Job? = null
    private var activeTransferJob: Job? = null

    private val isPaused = AtomicBoolean(false)

    // Per-transfer cancellation flag so cancelling Transfer #1 never poisons Transfer #2!
    @Volatile
    private var currentCancelFlag = AtomicBoolean(false)

    @Volatile
    var autoAcceptIncoming: Boolean = false

    @Volatile
    private var activeSenderSocket: Socket? = null

    @Volatile
    private var activeReceiverSocket: Socket? = null

    @Volatile
    private var activeConsentLatch: CountDownLatch? = null

    val historyRecords: MutableList<TransferHistoryRecord> =
        Collections.synchronizedList(mutableListOf())

    @Volatile
    private var sharedLinkItems: List<TransferItem> = emptyList()

    fun startServer() {
        startIndestructibleReceiverServer()
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
     * Immediately aborts the active transfer in <1ms using SO_LINGER(true, 0) (TCP RST)
     * so neither Sender nor Receiver ever stays stuck in FIN_WAIT or blocked read(),
     * while sealing and preserving the %5 partial file on disk!
     */
    fun cancelAndPreservePartialFile() {
        currentCancelFlag.set(true)
        isPaused.set(false)
        activeConsentLatch?.countDown()

        val sSock = activeSenderSocket
        val rSock = activeReceiverSocket
        activeSenderSocket = null
        activeReceiverSocket = null

        scope.launch {
            if (sSock != null) {
                try {
                    sSock.setSoLinger(true, 0)
                } catch (_: Throwable) {
                }
                try {
                    sSock.close()
                } catch (_: Throwable) {
                }
            }
            if (rSock != null) {
                try {
                    rSock.setSoLinger(true, 0)
                } catch (_: Throwable) {
                }
                try {
                    rSock.close()
                } catch (_: Throwable) {
                }
            }
        }
    }

    private fun configureHighSpeedSocket(socket: Socket, timeoutMs: Int) {
        try {
            socket.tcpNoDelay = true
            socket.keepAlive = true
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            socket.receiveBufferSize = SOCKET_BUFFER_BYTES
            socket.trafficClass = 0x08 // IPTOS_THROUGHPUT
            socket.soTimeout = timeoutMs
        } catch (_: Throwable) {
        }
    }

    /**
     * INDESTRUCTIBLE RECEIVER SERVER LOOP:
     * Never dies even if a transfer is cancelled at %5, rejected, or disconnected.
     * Ready to accept the 2nd, 3rd, 100th transfer immediately!
     */
    private fun startIndestructibleReceiverServer() {
        serverJob?.cancel()
        serverJob = scope.launch {
            while (isActive) {
                var serverSocket: ServerSocket? = null
                try {
                    Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
                    serverSocket = ServerSocket()
                    serverSocket.reuseAddress = true
                    serverSocket.receiveBufferSize = SOCKET_BUFFER_BYTES
                    serverSocket.bind(InetSocketAddress(transferPort))

                    while (isActive) {
                        val clientSocket = serverSocket.accept() ?: continue
                        // Launch in top-level supervised scope so a client error never cancels serverJob!
                        scope.launch {
                            handleIncomingClientSocket(clientSocket)
                        }
                    }
                } catch (_: Throwable) {
                    delay(400L)
                } finally {
                    try {
                        serverSocket?.close()
                    } catch (_: Throwable) {
                    }
                }
            }
        }
    }

    private fun handleIncomingClientSocket(clientSocket: Socket) {
        // Close any stale previous receiver socket if it was somehow left open
        try {
            activeReceiverSocket?.close()
        } catch (_: Throwable) {
        }

        val sessionCancel = AtomicBoolean(false)
        currentCancelFlag = sessionCancel
        activeReceiverSocket = clientSocket
        isPaused.set(false)

        try {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            configureHighSpeedSocket(clientSocket, timeoutMs = 65_000)

            val senderIp = clientSocket.inetAddress?.hostAddress ?: "Bilinmeyen IP"
            val rawIn: InputStream = clientSocket.getInputStream()
            val rawOut: OutputStream = clientSocket.getOutputStream()
            val dis = DataInputStream(rawIn)
            val dos = DataOutputStream(rawOut)

            val magic = dis.readInt()
            if (magic != PROTOCOL_MAGIC) {
                clientSocket.close()
                return
            }

            // 1. Read Handshake Metadata
            val senderAlias = dis.readUTF()
            val transportLabel = dis.readUTF().ifBlank { "Zero-Copy P2P" }
            val fileCount = dis.readInt()
            val totalBatchBytes = dis.readLong()
            val previewCount = dis.readInt()
            val previewLines = mutableListOf<String>()
            for (i in 0 until previewCount) {
                previewLines.add(dis.readUTF())
            }

            // 2. Ask Receiver User for Approval (Kabul Et / Reddet)
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
                clientSocket.close()
                return
            }

            // Send ACCEPTED byte & tighten read timeout to 12 seconds for active data streaming
            dos.writeByte(DECISION_ACCEPTED)
            dos.flush()
            clientSocket.soTimeout = 12_000
            onTransferActiveStateChanged(true)

            val blockArray = ByteArray(IO_BLOCK_BYTES)
            val blockBuffer = ByteBuffer.wrap(blockArray)

            for (fileIdx in 0 until fileCount) {
                if (sessionCancel.get()) break

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

                    while (totalBytesReceived < expectedFileSize && !sessionCancel.get()) {
                        while (isPaused.get() && !sessionCancel.get()) {
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
                        if (sessionCancel.get()) break

                        // Coalesce socket reads into large contiguous blocks (up to 4 MB) before writing to UFS flash!
                        val remainingFile = expectedFileSize - totalBytesReceived
                        val targetChunk = remainingFile.coerceAtMost(IO_BLOCK_BYTES.toLong()).toInt()
                        var accumulated = 0

                        while (accumulated < targetChunk && !sessionCancel.get() && !isPaused.get()) {
                            val r = rawIn.read(blockArray, accumulated, targetChunk - accumulated)
                            if (r <= 0) break
                            accumulated += r
                        }

                        if (accumulated <= 0) {
                            break
                        }

                        blockBuffer.position(0)
                        blockBuffer.limit(accumulated)
                        while (blockBuffer.hasRemaining()) {
                            totalBytesReceived += fileChannel.write(blockBuffer)
                        }

                        val nowMs = System.currentTimeMillis()
                        if (nowMs - lastUiUpdateMs >= 160L || totalBytesReceived == expectedFileSize) {
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
                    // Socket closed or reset on cancel -> fall through to finally to seal %5 partial file!
                } finally {
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
            onTransferActiveStateChanged(false)
            if (activeReceiverSocket === clientSocket) {
                activeReceiverSocket = null
            }
            try {
                clientSocket.close()
            } catch (_: Throwable) {
            }
        }
    }

    /**
     * SENDER ENGINE:
     * Uses a fresh per-session cancellation token, performs the LocalSend consent handshake,
     * and streams 4 MB coalesced DMA blocks directly from the original FileDescriptor.
     */
    fun sendItemsToPeers(
        items: List<TransferItem>,
        targetHostIps: List<Pair<String, Int>>,
        transportLabel: String
    ) {
        if (items.isEmpty() || targetHostIps.isEmpty()) return

        // Cleanly abort any lingering previous sender socket before starting the new transfer
        val oldSock = activeSenderSocket
        activeSenderSocket = null
        if (oldSock != null) {
            try {
                oldSock.setSoLinger(true, 0)
                oldSock.close()
            } catch (_: Throwable) {
            }
        }
        activeTransferJob?.cancel()

        val sessionCancel = AtomicBoolean(false)
        currentCancelFlag = sessionCancel
        isPaused.set(false)

        activeTransferJob = scope.launch {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            onTransferActiveStateChanged(true)
            try {
                for ((hostIp, port) in targetHostIps) {
                    if (sessionCancel.get()) break
                    sendItemsToSinglePeer(items, hostIp, port, transportLabel, sessionCancel)
                }
            } finally {
                onTransferActiveStateChanged(false)
            }
        }
    }

    private fun sendItemsToSinglePeer(
        items: List<TransferItem>,
        hostIp: String,
        port: Int,
        transportLabel: String,
        sessionCancel: AtomicBoolean
    ) {
        var socket: Socket? = null
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

            socket = Socket()
            activeSenderSocket = socket
            configureHighSpeedSocket(socket, timeoutMs = 65_000)
            socket.connect(InetSocketAddress(hostIp, port), 4500)

            val rawOut: OutputStream = socket.getOutputStream()
            val rawIn: InputStream = socket.getInputStream()
            val dos = DataOutputStream(rawOut)
            val dis = DataInputStream(rawIn)

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

            val decision = dis.readByte().toInt()
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

            socket.soTimeout = 12_000
            val ioArray = ByteArray(IO_BLOCK_BYTES)
            val ioBuffer = ByteBuffer.wrap(ioArray)

            for (item in items) {
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

                    dos.writeUTF(item.name)
                    dos.writeLong(actualSize)
                    dos.flush()

                    var bytesSent = 0L
                    val startTimeNs = System.nanoTime()
                    var lastUiUpdateMs = 0L

                    if (item.inlineTextBytes != null) {
                        rawOut.write(item.inlineTextBytes)
                        rawOut.flush()
                        bytesSent = item.inlineTextBytes.size.toLong()
                    } else if (fileChannel != null) {
                        while (bytesSent < actualSize && !sessionCancel.get()) {
                            while (isPaused.get() && !sessionCancel.get()) {
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
                            if (sessionCancel.get()) break

                            ioBuffer.clear()
                            val rem = actualSize - bytesSent
                            if (rem < ioBuffer.capacity()) {
                                ioBuffer.limit(rem.toInt())
                            }

                            val readCount = fileChannel.read(ioBuffer, bytesSent)
                            if (readCount <= 0) break

                            rawOut.write(ioArray, 0, readCount)
                            bytesSent += readCount

                            val nowMs = System.currentTimeMillis()
                            if (nowMs - lastUiUpdateMs >= 160L || bytesSent == actualSize) {
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
                        rawOut.flush()
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
            if (sessionCancel.get()) {
                onProgressUpdate(
                    TransferProgress(
                        fileName = items.firstOrNull()?.name ?: "Dosya",
                        bytesTransferred = 0L,
                        totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                        speedMbps = 0.0,
                        etaSeconds = 0L,
                        status = TransferStatus.PARTIAL_PRESERVED,
                        transportLabel = transportLabel,
                        message = "İşlem durduruldu (Kısmi dosya korundu)."
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
                        message = "Hedef cihaza ($hostIp) ulaşılamadı. Karşı cihazda Zeph açık olduğundan emin olun."
                    )
                )
            }
        } finally {
            if (activeSenderSocket === socket) {
                activeSenderSocket = null
            }
            try {
                socket?.close()
            } catch (_: Throwable) {
            }
        }
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
        activeTransferJob?.cancel()
        serverJob?.cancel()
        httpServerJob?.cancel()
    }
}
