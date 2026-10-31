package com.fasttransfer.p2p.util

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import android.content.pm.ApplicationInfo
import android.content.pm.PackageManager
import android.media.MediaScannerConnection
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.OpenableColumns
import android.webkit.MimeTypeMap
import androidx.documentfile.provider.DocumentFile
import com.fasttransfer.p2p.model.ApkAppItem
import com.fasttransfer.p2p.model.TransferItem
import java.io.File
import java.text.DecimalFormat
import java.util.UUID

object StorageHelper {

    /**
     * ZERO-CACHE SHARE SHEET & PICKER URI RESOLVER:
     * Never copies any URI to cacheDir or filesDir!
     * Reads only metadata (name & size) in <1ms and keeps the original content:// or file:// URI
     * so ZeroCopyTransferEngine streams directly from the underlying FileDescriptor to the NIC.
     */
    fun extractTransferItemsFromIntent(context: Context, intent: Intent?): List<TransferItem> {
        if (intent == null) return emptyList()
        val action = intent.action ?: return emptyList()
        val results = mutableListOf<TransferItem>()

        when (action) {
            Intent.ACTION_SEND -> {
                val singleUri = CompatibilityHelper.getParcelableExtraCompat(
                    intent,
                    Intent.EXTRA_STREAM,
                    Uri::class.java
                )
                if (singleUri != null) {
                    buildTransferItemFromUri(context, singleUri)?.let { results.add(it) }
                } else {
                    val sharedText = intent.getStringExtra(Intent.EXTRA_TEXT)
                    if (!sharedText.isNullOrEmpty()) {
                        results.add(buildTransferItemFromText(sharedText))
                    }
                }
            }
            Intent.ACTION_SEND_MULTIPLE -> {
                val uriList = CompatibilityHelper.getParcelableArrayListExtraCompat(
                    intent,
                    Intent.EXTRA_STREAM,
                    Uri::class.java
                )
                if (!uriList.isNullOrEmpty()) {
                    for (uri in uriList) {
                        buildTransferItemFromUri(context, uri)?.let { results.add(it) }
                    }
                } else {
                    val clipData: ClipData? = intent.clipData
                    if (clipData != null) {
                        for (i in 0 until clipData.itemCount) {
                            val u = clipData.getItemAt(i).uri
                            if (u != null) {
                                buildTransferItemFromUri(context, u)?.let { results.add(it) }
                            }
                        }
                    }
                }
            }
        }
        return results
    }

    fun buildTransferItemFromUri(context: Context, uri: Uri): TransferItem? {
        if (uri.scheme == "file") {
            val path = uri.path ?: return null
            val f = File(path)
            if (!f.exists()) return null
            return TransferItem(
                id = UUID.randomUUID().toString(),
                name = sanitizeFileName(f.name),
                sizeBytes = f.length(),
                mimeType = guessMimeType(f.name),
                uri = uri,
                directFile = f
            )
        }

        var displayName = "paylasilan_dosya_${System.currentTimeMillis()}"
        var sizeBytes = -1L

        try {
            context.contentResolver.query(
                uri,
                arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE),
                null,
                null,
                null
            )?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val nameIdx = cursor.getColumnIndex(OpenableColumns.DISPLAY_NAME)
                    val sizeIdx = cursor.getColumnIndex(OpenableColumns.SIZE)
                    if (nameIdx != -1 && !cursor.isNull(nameIdx)) {
                        displayName = cursor.getString(nameIdx)
                    }
                    if (sizeIdx != -1 && !cursor.isNull(sizeIdx)) {
                        sizeBytes = cursor.getLong(sizeIdx)
                    }
                }
            }
        } catch (_: Throwable) {
        }

        if (sizeBytes <= 0L) {
            try {
                context.contentResolver.openFileDescriptor(uri, "r")?.use { pfd ->
                    if (pfd.statSize > 0L) {
                        sizeBytes = pfd.statSize
                    }
                }
            } catch (_: Throwable) {
            }
        }

        val resolvedMime = context.contentResolver.getType(uri) ?: guessMimeType(displayName)
        return TransferItem(
            id = UUID.randomUUID().toString(),
            name = sanitizeFileName(displayName),
            sizeBytes = if (sizeBytes > 0L) sizeBytes else 0L,
            mimeType = resolvedMime,
            uri = uri
        )
    }

    fun buildTransferItemsFromTreeUri(context: Context, treeUri: Uri): List<TransferItem> {
        val results = mutableListOf<TransferItem>()
        val rootDoc = DocumentFile.fromTreeUri(context, treeUri) ?: return results
        collectDocumentFilesRecursively(rootDoc, results)
        return results
    }

    private fun collectDocumentFilesRecursively(
        dir: DocumentFile,
        outList: MutableList<TransferItem>
    ) {
        for (file in dir.listFiles()) {
            if (file.isDirectory) {
                collectDocumentFilesRecursively(file, outList)
            } else if (file.isFile) {
                val name = sanitizeFileName(file.name ?: "dosya_${System.currentTimeMillis()}")
                outList.add(
                    TransferItem(
                        id = UUID.randomUUID().toString(),
                        name = name,
                        sizeBytes = file.length(),
                        mimeType = file.type ?: guessMimeType(name),
                        uri = file.uri
                    )
                )
            }
        }
    }

    fun buildTransferItemFromText(text: String): TransferItem {
        val bytes = text.toByteArray(Charsets.UTF_8)
        val timestamp = System.currentTimeMillis() % 100000
        return TransferItem(
            id = UUID.randomUUID().toString(),
            name = "Mesaj_$timestamp.txt",
            sizeBytes = bytes.size.toLong(),
            mimeType = "text/plain",
            inlineTextBytes = bytes
        )
    }

    fun readClipboardAsTransferItem(context: Context): TransferItem? {
        val cm = context.getSystemService(Context.CLIPBOARD_SERVICE) as? ClipboardManager ?: return null
        val clip = cm.primaryClip ?: return null
        if (clip.itemCount == 0) return null
        val item = clip.getItemAt(0)
        val uri = item.uri
        if (uri != null) {
            return buildTransferItemFromUri(context, uri)
        }
        val text = item.coerceToText(context)?.toString()
        if (!text.isNullOrBlank()) {
            return buildTransferItemFromText(text)
        }
        return null
    }

    @Suppress("DEPRECATION")
    fun loadInstalledApks(context: Context): List<ApkAppItem> {
        val pm = context.packageManager
        val packages = try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                pm.getInstalledPackages(PackageManager.PackageInfoFlags.of(0L))
            } else {
                pm.getInstalledPackages(0)
            }
        } catch (_: Throwable) {
            emptyList()
        }

        val apkItems = mutableListOf<ApkAppItem>()
        for (pkg in packages) {
            val appInfo = pkg.applicationInfo ?: continue
            val sourcePath = appInfo.sourceDir ?: continue
            val apkFile = File(sourcePath)
            if (!apkFile.exists() || !apkFile.canRead()) continue

            // Include user-installed apps and updated system apps (just like LocalSend APK picker)
            val isSystem = (appInfo.flags and ApplicationInfo.FLAG_SYSTEM) != 0
            val isUpdatedSystem = (appInfo.flags and ApplicationInfo.FLAG_UPDATED_SYSTEM_APP) != 0
            if (isSystem && !isUpdatedSystem && pm.getLaunchIntentForPackage(pkg.packageName) == null) {
                continue
            }

            val label = try {
                appInfo.loadLabel(pm).toString()
            } catch (_: Throwable) {
                pkg.packageName
            }
            val icon = try {
                appInfo.loadIcon(pm)
            } catch (_: Throwable) {
                null
            }
            val version = pkg.versionName ?: "1.0"
            apkItems.add(
                ApkAppItem(
                    appName = label,
                    packageName = pkg.packageName,
                    versionName = version,
                    apkFile = apkFile,
                    sizeBytes = apkFile.length(),
                    icon = icon,
                    isSelected = false
                )
            )
        }
        return apkItems.sortedBy { it.appName.lowercase() }
    }

    /**
     * DIRECT PUBLIC DOWNLOADS TARGET:
     * Never uses .tmp or .part extensions! Writes directly with the real filename and real extension
     * into Environment.DIRECTORY_DOWNLOADS so the user can immediately open even a %5 partial file.
     */
    @Suppress("DEPRECATION")
    fun createDirectDownloadTarget(context: Context, originalFileName: String): File {
        val cleanName = sanitizeFileName(originalFileName)
        val downloadsDir = try {
            val pubDir = Environment.getExternalStoragePublicDirectory(Environment.DIRECTORY_DOWNLOADS)
            if (!pubDir.exists()) {
                pubDir.mkdirs()
            }
            if (pubDir.exists() && pubDir.canWrite()) {
                pubDir
            } else {
                context.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: context.filesDir
            }
        } catch (_: Throwable) {
            context.getExternalFilesDir(Environment.DIRECTORY_DOWNLOADS) ?: context.filesDir
        }

        var candidate = File(downloadsDir, cleanName)
        if (!candidate.exists()) {
            return candidate
        }

        val dotIndex = cleanName.lastIndexOf('.')
        val baseName = if (dotIndex > 0) cleanName.substring(0, dotIndex) else cleanName
        val ext = if (dotIndex > 0) cleanName.substring(dotIndex) else ""
        var counter = 1
        while (candidate.exists()) {
            candidate = File(downloadsDir, "${baseName}_($counter)$ext")
            counter++
        }
        return candidate
    }

    fun notifyMediaScanner(context: Context, file: File) {
        try {
            val mime = guessMimeType(file.name)
            MediaScannerConnection.scanFile(
                context.applicationContext,
                arrayOf(file.absolutePath),
                arrayOf(mime),
                null
            )
        } catch (_: Throwable) {
        }
    }

    fun sanitizeFileName(raw: String): String {
        val replaced = raw.replace(Regex("[\\/:*?"<>|]"), "_").trim()
        return if (replaced.isEmpty()) "dosya_${System.currentTimeMillis()}.bin" else replaced
    }

    fun guessMimeType(fileName: String): String {
        val dot = fileName.lastIndexOf('.')
        if (dot != -1 && dot < fileName.length - 1) {
            val ext = fileName.substring(dot + 1).lowercase()
            val mime = MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext)
            if (!mime.isNullOrEmpty()) return mime
            return when (ext) {
                "mkv" -> "video/x-matroska"
                "mp4", "m4v" -> "video/mp4"
                "mov" -> "video/quicktime"
                "avi" -> "video/x-msvideo"
                "webm" -> "video/webm"
                "mp3" -> "audio/mpeg"
                "flac" -> "audio/flac"
                "apk" -> "application/vnd.android.package-archive"
                "pdf" -> "application/pdf"
                "zip" -> "application/zip"
                else -> "application/octet-stream"
            }
        }
        return "application/octet-stream"
    }

    fun formatFileSize(bytes: Long): String {
        if (bytes <= 0L) return "0 B"
        val units = arrayOf("B", "KB", "MB", "GB", "TB")
        val digitGroups = (Math.log10(bytes.toDouble()) / Math.log10(1024.0)).toInt().coerceIn(0, 4)
        val value = bytes / Math.pow(1024.0, digitGroups.toDouble())
        return DecimalFormat("#,##0.#").format(value) + " " + units[digitGroups]
    }
}
