package com.fasttransfer.p2p.model

import android.graphics.drawable.Drawable
import android.net.Uri
import java.io.File

enum class DeviceCategory {
    PHONE,
    LAPTOP,
    TERMINAL,
    SERVER
}

enum class ConnectionTransport(val label: String, val priority: Int) {
    WIFI_DIRECT_5GHZ("5 GHz Doğrudan Köprü (Maks Hız)", 1),
    WIFI_DIRECT_2_4GHZ("2.4 GHz Doğrudan Köprü", 2),
    LAN_HOTSPOT("Yerel Ağ / Modem IP", 3)
}

enum class SendMode {
    SINGLE_RECIPIENT,
    MULTI_RECIPIENT,
    SHARE_VIA_LINK
}

enum class TransferStatus {
    IDLE,
    CONNECTING,
    WAITING_APPROVAL,
    TRANSFERRING,
    PAUSED,
    COMPLETED,
    PARTIAL_PRESERVED,
    REJECTED,
    ERROR
}

data class TransferItem(
    val id: String,
    val name: String,
    val sizeBytes: Long,
    val mimeType: String,
    val uri: Uri? = null,
    val directFile: File? = null,
    val inlineTextBytes: ByteArray? = null
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is TransferItem) return false
        return id == other.id
    }

    override fun hashCode(): Int = id.hashCode()
}

data class PeerDevice(
    val id: String,
    val alias: String,
    val ipAddress: String?,
    val macAddress: String?,
    val port: Int = 53317,
    val category: DeviceCategory = DeviceCategory.PHONE,
    val transport: ConnectionTransport = ConnectionTransport.LAN_HOTSPOT,
    val isFavorite: Boolean = false,
    val lastSeenMs: Long = System.currentTimeMillis()
)

data class IncomingTransferRequest(
    val senderAlias: String,
    val senderIp: String,
    val transportLabel: String,
    val fileCount: Int,
    val totalBytes: Long,
    val filePreviewLines: String,
    val onDecision: (Boolean) -> Unit
)

data class TransferProgress(
    val fileName: String,
    val bytesTransferred: Long,
    val totalBytes: Long,
    val speedMbps: Double,
    val etaSeconds: Long,
    val status: TransferStatus,
    val transportLabel: String,
    val savedFilePath: String? = null,
    val message: String? = null
) {
    val progressPercent: Int
        get() = if (totalBytes > 0L) {
            ((bytesTransferred * 100L) / totalBytes).toInt().coerceIn(0, 100)
        } else {
            0
        }
}

data class ApkAppItem(
    val appName: String,
    val packageName: String,
    val versionName: String,
    val apkFile: File,
    val sizeBytes: Long,
    val icon: Drawable?,
    var isSelected: Boolean = false
)

data class TransferHistoryRecord(
    val fileName: String,
    val filePath: String,
    val bytesWritten: Long,
    val totalExpectedBytes: Long,
    val isPartial: Boolean,
    val timestampMs: Long
)
