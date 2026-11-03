// Zeph v21.0.0-V12TurboCore (Revision 2026.11.04-v21)
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
import android.os.ParcelFileDescriptor
import android.provider.DocumentsContract
import android.provider.MediaStore
import android.provider.OpenableColumns
import android.system.Os
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
     * Resolves the underlying direct File path (/storage/emulated/0/...) whenever accessible
     * (via /proc/self/fd readlink, DocumentsContract, or MediaStore DATA) to bypass Android's
     * MediaProvider FUSE daemon overhead completely!
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

    private fun tryResolveDirectReadableFile(
        context: Context,
        uri: Uri,
        openPfd: ParcelFileDescriptor? = null
    ): File? {
        if (uri.scheme == "file") {
            val f = uri.path?.let { File(it) }
            if (f != null && f.exists() && f.isFile && f.canRead()) return f
        }

        // 1. Try /proc/self/fd/<fd> symlink resolution
        if (openPfd != null) {
            try {
                val rawLink = Os.readlink("/proc/self/fd/${openPfd.fd}")
                if (!rawLink.isNullOrBlank()) {
                    val candidates = mutableListOf(File(rawLink))
                    val emulatedSuffix = rawLink.substringAfter("/emulated/0/", "")
                    if (emulatedSuffix.isNotEmpty()) {
                        candidates.add(File("/storage/emulated/0/$emulatedSuffix"))
                    }
                    for (c in candidates) {
                        if (c.exists() && c.isFile && c.canRead()) {
                            return c
                        }
                    }
                }
            } catch (_: Throwable) {
            }
        }

        // 2. Try ExternalStorageProvider document ID (e.g. primary:Download/birlesmis_video.mp4)
        try {
            if (DocumentsContract.isDocumentUri(context, uri) &&
                uri.authority == "com.android.externalstorage.documents"
            ) {
                val docId = DocumentsContract.getDocumentId(uri)
                val parts = docId.split(":")
                if (parts.size >= 2) {
                    val type = parts[0]
                    val relPath = parts[1]
                    if (type.equals("primary", ignoreCase = true)) {
                        @Suppress("DEPRECATION")
                        val f = File(Environment.getExternalStorageDirectory(), relPath)
                        if (f.exists() && f.isFile && f.canRead()) {
                            return f
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }

        // 3. Try MediaStore.MediaColumns.DATA column
        try {
            @Suppress("DEPRECATION")
            val dataCol = MediaStore.MediaColumns.DATA
            context.contentResolver.query(uri, arrayOf(dataCol), null, null, null)?.use { cursor ->
                if (cursor.moveToFirst()) {
                    val idx = cursor.getColumnIndex(dataCol)
                    if (idx != -1 && !cursor.isNull(idx)) {
                        val path = cursor.getString(idx)
                        if (!path.isNullOrBlank()) {
                            val f = File(path)
                            if (f.exists() && f.isFile && f.canRead()) {
                                return f
                            }
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }

        return null
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

        var resolvedDirectFile: File? = tryResolveDirectReadableFile(context, uri, null)

        if (sizeBytes <= 0L || resolvedDirectFile == null) {
            try {
                context.contentResolver.openFileDescriptor(uri, "r")?.use { pfd ->
                    if (sizeBytes <= 0L && pfd.statSize > 0L) {
                        sizeBytes = pfd.statSize
                    }
                    if (resolvedDirectFile == null) {
                        resolvedDirectFile = tryResolveDirectReadableFile(context, uri, pfd)
                    }
                }
            } catch (_: Throwable) {
            }
        }

        if (resolvedDirectFile != null && sizeBytes <= 0L) {
            sizeBytes = resolvedDirectFile!!.length()
        }

        val resolvedMime = context.contentResolver.getType(uri) ?: guessMimeType(displayName)
        return TransferItem(
            id = UUID.randomUUID().toString(),
            name = sanitizeFileName(displayName),
            sizeBytes = if (sizeBytes > 0L) sizeBytes else 0L,
            mimeType = resolvedMime,
            uri = uri,
            directFile = resolvedDirectFile
        )
    }

    fun buildTransferItemsFromTreeUri(context: Context, treeUri: Uri): List<TransferItem> {
        val results = mutableListOf<TransferItem>()
        val rootDoc = DocumentFile.fromTreeUri(context, treeUri) ?: return results
        collectDocumentFilesRecursively(context, rootDoc, results)
        return results
    }

    private fun collectDocumentFilesRecursively(
        context: Context,
        dir: DocumentFile,
        outList: MutableList<TransferItem>
    ) {
        for (file in dir.listFiles()) {
            if (file.isDirectory) {
                collectDocumentFilesRecursively(context, file, outList)
            } else if (file.isFile) {
                val name = sanitizeFileName(file.name ?: "dosya_${System.currentTimeMillis()}")
                val direct = tryResolveDirectReadableFile(context, file.uri, null)
                outList.add(
                    TransferItem(
                        id = UUID.randomUUID().toString(),
                        name = name,
                        sizeBytes = file.length(),
                        mimeType = file.type ?: guessMimeType(name),
                        uri = file.uri,
                        directFile = direct
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
        val replaced = raw.replace(Regex("[^a-zA-Z0-9._-]"), "_").trim()
        return replaced.ifEmpty { "dosya_${System.currentTimeMillis()}" }
    }

    fun guessMimeType(fileName: String): String {
        val ext = fileName.substringAfterLast('.', "").lowercase()
        if (ext.isNotEmpty()) {
            val mapped = MimeTypeMap.getSingleton().getMimeTypeFromExtension(ext)
            if (!mapped.isNullOrEmpty()) return mapped
        }
        return when (ext) {
            "mp4", "m4v" -> "video/mp4"
            "mkv" -> "video/x-matroska"
            "mov" -> "video/quicktime"
            "avi" -> "video/x-msvideo"
            "webm" -> "video/webm"
            "mp3" -> "audio/mpeg"
            "wav" -> "audio/wav"
            "flac" -> "audio/flac"
            "jpg", "jpeg" -> "image/jpeg"
            "png" -> "image/png"
            "gif" -> "image/gif"
            "webp" -> "image/webp"
            "pdf" -> "application/pdf"
            "apk" -> "application/vnd.android.package-archive"
            "zip" -> "application/zip"
            "txt", "log", "md" -> "text/plain"
            else -> "application/octet-stream"
        }
    }

    fun formatFileSize(bytes: Long): String {
        if (bytes <= 0L) return "0 B"
        val kb = 1024.0
        val mb = kb * 1024.0
        val gb = mb * 1024.0
        val df1 = DecimalFormat("0.0")
        val df2 = DecimalFormat("0.00")
        return when {
            bytes >= gb -> "${df2.format(bytes / gb)} GB"
            bytes >= mb -> "${df1.format(bytes / mb)} MB"
            bytes >= kb -> "${df1.format(bytes / kb)} KB"
            else -> "$bytes B"
        }
    }
}
