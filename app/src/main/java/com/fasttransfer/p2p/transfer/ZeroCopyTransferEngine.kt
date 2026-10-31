package com.fasttransfer.p2p.transfer

import android.content.Context
import android.os.Build
import android.os.ParcelFileDescriptor
import android.os.Process
import android.system.Os
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
import java.util.concurrent.atomic.AtomicBoolean

/**
 * KERNEL ZERO-COPY & %5 PARTIAL FILE PRESERVATION ENGINE:
 * - Uses 8 MB TCP Send/Receive buffers (SO_SNDBUF = 8MB, SO_RCVBUF = 8MB) and TCP_NODELAY = true.
 * - Never splits files into part1/part2 and never merges at the end (protects UFS/eMMC lifespan).
 * - Never caches shared URIs to cacheDir/filesDir; reads directly from ParcelFileDescriptor via FileChannel.transferTo().
 * - Writes incoming files directly into public Downloads with their real filename and extension.
 * - Pre-allocates file space with Os.posix_fallocate() when supported, and if paused/cancelled/disconnected at ANY
 *   percentage (e.g. %5), NEVER calls dosya silme metodu! Immediately calls fileChannel.truncate(totalBytesReceived),
 *   fileChannel.force(true) (fsync), and MediaScannerConnection.scanFile() so the %5 video is playable immediately.
 */
class ZeroCopyTransferEngine(
    private val context: Context,
    private val transferPort: Int = 53317,
    private val httpLinkPort: Int = 53319,
    private val onProgressUpdate: (TransferProgress) -> Unit,
    private val onHistoryAdded: (TransferHistoryRecord) -> Unit
) {
    companion object {
        const val SOCKET_BUFFER_BYTES = 8 * 1024 * 1024 // 8 MB TCP Window
        const val DIRECT_BUFFER_BYTES = 4 * 1024 * 1024 // 4 MB Direct DMA Buffer
        const val ZERO_COPY_CHUNK_BYTES = 8L * 1024L * 1024L // 8 MB per transferTo syscall
        private const val PROTOCOL_MAGIC = 0x46545032 // "FTP2"
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var serverJob: Job? = null
    private var httpServerJob: Job? = null
    private var activeTransferJob: Job? = null

    private val isPaused = AtomicBoolean(false)
    private val isCancelled = AtomicBoolean(false)

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

    fun togglePauseResume(): Boolean {
        val nextState = !isPaused.get()
        isPaused.set(nextState)
        return nextState
    }

    fun isCurrentlyPaused(): Boolean = isPaused.get()

    /**
     * Stops the current transfer immediately while strictly preserving the partial file on disk.
     */
    fun cancelAndPreservePartialFile() {
        isCancelled.set(true)
        isPaused.set(false)
    }

    private fun configureChannelSocketBuffers(channel: SocketChannel) {
        try {
            val socket = channel.socket()
            socket.tcpNoDelay = true
            socket.keepAlive = true
            socket.sendBufferSize = SOCKET_BUFFER_BYTES
            socket.receiveBufferSize = SOCKET_BUFFER_BYTES
            socket.trafficClass = 0x10 // IPTOS_LOWDELAY
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

    /**
     * RECEIVER SERVER LOOP:
     * Accepts incoming NIO SocketChannel connections, reads binary header, allocates space in public Downloads,
     * streams sequential bytes via 4MB Direct ByteBuffer, and seals partial or full files with truncate() + force(true).
     */
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
        isCancelled.set(false)
        isPaused.set(false)
        try {
            Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
            clientChannel.configureBlocking(true)
            configureChannelSocketBuffers(clientChannel)

            val dis = DataInputStream(clientChannel.socket().getInputStream())
            val magic = dis.readInt()
            if (magic != PROTOCOL_MAGIC) {
                clientChannel.close()
                return
            }

            val fileCount = dis.readInt()
            val directBuf = ByteBuffer.allocateDirect(DIRECT_BUFFER_BYTES)

            for (fileIdx in 0 until fileCount) {
                if (isCancelled.get()) break

                val fileName = dis.readUTF()
                val expectedFileSize = dis.readLong()
                val transportLabel = dis.readUTF().ifBlank { "Zero-Copy P2P" }

                val targetFile: File = StorageHelper.createDirectDownloadTarget(context, fileName)
                var totalBytesReceived = 0L
                val startTimeNs = System.nanoTime()
                var lastUiUpdateMs = 0L

                var raf: RandomAccessFile? = null
                var fileChannel: FileChannel? = null

                try {
                    raf = RandomAccessFile(targetFile, "rw")
                    fileChannel = raf.channel

                    // Pre-allocate contiguous UFS/eMMC blocks if supported by OS/Filesystem
                    if (expectedFileSize > 0L) {
                        try {
                            Os.posix_fallocate(raf.fd, 0L, expectedFileSize)
                        } catch (_: Throwable) {
                            // Safe fallback to sequential write on older devices/FAT32
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
                            Thread.sleep(120L)
                        }
                        if (isCancelled.get()) break

                        directBuf.clear()
                        val remaining = expectedFileSize - totalBytesReceived
                        if (remaining < directBuf.capacity()) {
                            directBuf.limit(remaining.toInt())
                        }

                        val bytesRead = clientChannel.read(directBuf)
                        if (bytesRead <= 0) {
                            // Connection dropped or EOF reached early -> preserve partial file!
                            break
                        }

                        directBuf.flip()
                        while (directBuf.hasRemaining()) {
                            totalBytesReceived += fileChannel.write(directBuf)
                        }

                        val nowMs = System.currentTimeMillis()
                        if (nowMs - lastUiUpdateMs >= 100L || totalBytesReceived == expectedFileSize) {
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
                    // Any socket drop or interruption falls through to finally block to seal the partial file!
                } finally {
                    // =========================================================================
                    // CRITICAL %5 PARTIAL FILE PRESERVATION & SEALING RULE:
                    // 1) NEVER call hedef dosya silme metodu!
                    // 2) Truncate fileChannel to totalBytesReceived so unwritten fallocate zeros
                    //    at the end are removed and video players immediately play the %5 clip!
                    // 3) Call fileChannel.force(true) (fsync) to flush kernel buffers to UFS/eMMC.
                    // 4) Trigger MediaScannerConnection.scanFile() immediately.
                    // =========================================================================
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
                                    "Kısmi Dosya Mühürlendi (%$pct • ${StorageHelper.formatFileSize(totalBytesReceived)}) - Anında Oynatılabilir!"
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
            try {
                clientChannel.close()
            } catch (_: Throwable) {
            }
        }
    }

    /**
     * SENDER ENGINE:
     * Streams one or more TransferItems directly from disk/URI to one or more peer IPs
     * using Kernel Zero-Copy (FileChannel.transferTo) with automatic 4MB Direct ByteBuffer fallback.
     */
    fun sendItemsToPeers(
        items: List<TransferItem>,
        targetHostIps: List<Pair<String, Int>>,
        transportLabel: String
    ) {
        if (items.isEmpty() || targetHostIps.isEmpty()) return
        isCancelled.set(false)
        isPaused.set(false)

        activeTransferJob?.cancel()
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
            onProgressUpdate(
                TransferProgress(
                    fileName = items.first().name,
                    bytesTransferred = 0L,
                    totalBytes = items.first().sizeBytes,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    status = TransferStatus.CONNECTING,
                    transportLabel = transportLabel,
                    message = "$hostIp:$port bağlanıyor..."
                )
            )

            socketChannel = SocketChannel.open()
            socketChannel.configureBlocking(true)
            configureChannelSocketBuffers(socketChannel)
            socketChannel.socket().connect(InetSocketAddress(hostIp, port), 6000)

            val dos = DataOutputStream(socketChannel.socket().getOutputStream())
            dos.writeInt(PROTOCOL_MAGIC)
            dos.writeInt(items.size)
            dos.flush()

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
                        // ZERO-CACHE DIRECT STREAMING FROM ORIGINAL URI DESCRIPTOR
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
                    dos.writeUTF(transportLabel)
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
                                Thread.sleep(120L)
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
                                } catch (_: Throwable) {
                                    zeroCopySupported = false
                                }
                            }

                            if (!zeroCopySupported) {
                                fallbackDirectBuffer.clear()
                                val rem = actualSize - bytesSent
                                if (rem < fallbackDirectBuffer.capacity()) {
                                    fallbackDirectBuffer.limit(rem.toInt())
                                }
                                val readCount = fileChannel.read(fallbackDirectBuffer, bytesSent)
                                if (readCount <= 0) break
                                fallbackDirectBuffer.flip()
                                while (fallbackDirectBuffer.hasRemaining()) {
                                    val written = socketChannel.write(fallbackDirectBuffer)
                                    if (written <= 0) break
                                    bytesSent += written
                                }
                            }

                            val nowMs = System.currentTimeMillis()
                            if (nowMs - lastUiUpdateMs >= 100L || bytesSent == actualSize) {
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
                                "Gönderim %$pct noktasında durduruldu (Karşı cihazda %$pct kısmi dosya korundu)"
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
            onProgressUpdate(
                TransferProgress(
                    fileName = items.firstOrNull()?.name ?: "Dosya",
                    bytesTransferred = 0L,
                    totalBytes = items.firstOrNull()?.sizeBytes ?: 0L,
                    speedMbps = 0.0,
                    etaSeconds = 0L,
                    status = TransferStatus.ERROR,
                    transportLabel = transportLabel,
                    message = "Bağlantı Kesildi: ${t.localizedMessage ?: "Hedef cihaza ulaşılamadı"}"
                )
            )
        } finally {
            try {
                socketChannel?.close()
            } catch (_: Throwable) {
            }
        }
    }

    /**
     * BUILT-IN HTTP LINK SERVER ("Link üzerinden paylaşın" & "Link üzerinden al" in Video 00:02, 00:36):
     * Allows any device or browser on the local network/hotspot to download selected files via HTTP Zero-Cache stream.
     */
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
                        append("HTTP/1.1 200 OK
")
                        append("Content-Type: ${item.mimeType}
")
                        if (item.sizeBytes > 0L) {
                            append("Content-Length: ${item.sizeBytes}
")
                        }
                        append("Content-Disposition: attachment; filename="$encodedName"
")
                        append("Connection: close

")
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

            // Default HTML index listing all shared files
            val html = buildString {
                append("<!DOCTYPE html><html><head><meta charset='utf-8'>")
                append("<meta name='viewport' content='width=device-width,initial-scale=1'>")
                append("<title>FastTransfer Link Paylaşımı</title>")
                append("<style>body{background:#101818;color:#ECF4F6;font-family:sans-serif;padding:24px;}")
                append(".card{background:#182326;padding:16px;border-radius:14px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;}")
                append("a.btn{background:#6CD4FF;color:#002A3A;padding:10px 18px;border-radius:20px;text-decoration:none;font-weight:bold;}</style></head><body>")
                append("<h2>FastTransfer • Link Üzerinden Paylaşım</h2>")
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
            val responseHeader = "HTTP/1.1 200 OK
Content-Type: text/html; charset=utf-8
Content-Length: ${bodyBytes.size}
Connection: close

"
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
        isCancelled.set(true)
        activeTransferJob?.cancel()
        serverJob?.cancel()
        httpServerJob?.cancel()
    }
}
