package com.fasttransfer.p2p.model

import android.graphics.drawable.Drawable
import android.net.Uri
import java.io.File

enum class AppThemeMode(val prefValue: Int, val label: String) {
    AMOLED_BLACK(0, "AMOLED Siyah"),
    DARK_SLATE(1, "Karanlık Tema"),
    LIGHT_CLEAN(2, "Açık Tema");

    companion object {
        fun fromPref(value: Int): AppThemeMode = values().firstOrNull { it.prefValue == value } ?: DARK_SLATE
    }
}

enum class DeviceCategory {
    PHONE,
    LAPTOP,
    TERMINAL,
    SERVER
}

enum class ConnectionTransport(val label: String, val priority: Int) {
    WIFI_DIRECT_AUTO("Doğrudan Köprü (6GHz / 5GHz)", 1),
    LAN_HOTSPOT("Otomatik Yüksek Hızlı Köprü", 2)
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

data class P2pGroupCredentials(
    val ssid: String,
    val passphrase: String,
    val ownerIp: String = "192.168.49.1",
    val freqMhz: Int = 5180,
    val bandLabel: String = "5 GHz Doğrudan Köprü"
)

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
    val deviceModel: String = "Android",
    val ipAddress: String?,
    val macAddress: String?,
    val port: Int = 53317,
    val category: DeviceCategory = DeviceCategory.PHONE,
    val transport: ConnectionTransport = ConnectionTransport.LAN_HOTSPOT,
    val isFavorite: Boolean = false,
    val p2pSsid: String? = null,
    val p2pPassphrase: String? = null,
    val p2pFreqMhz: Int = 0,
    val lastSeenMs: Long = System.currentTimeMillis()
)

data class IncomingTransferRequest(
    val senderAlias: String,
    val senderModel: String,
    val senderIp: String,
    val transportLabel: String,
    val fileCount: Int,
    val totalBytes: Long,
    val firstFileName: String,
    val firstFileSize: Long,
    val filePreviewLines: String,
    val onDecision: (Boolean) -> Unit
)

data class TransferProgress(
    val fileName: String,
    val fileIndex: Int = 0,
    val totalFiles: Int = 1,
    val bytesTransferred: Long,
    val totalBytes: Long,
    val speedMbps: Double,
    val etaSeconds: Long,
    val elapsedMillis: Long = 0L,
    val status: TransferStatus,
    val transportLabel: String,
    val isSending: Boolean = false,
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
    val elapsedMillis: Long = 0L,
    val avgSpeedMbps: Double = 0.0,
    val isPartial: Boolean,
    val timestampMs: Long
)
