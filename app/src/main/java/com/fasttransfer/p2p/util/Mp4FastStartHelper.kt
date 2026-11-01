package com.fasttransfer.p2p.util

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel

object Mp4FastStartHelper {

    data class Mp4TailMoovMetadata(
        val mdatOffset: Long,
        val mdatHeaderSize: Int,
        val originalMdatHeader: ByteArray,
        val tailMoovBytes: ByteArray
    )

    fun isMp4FamilyFile(fileName: String): Boolean {
        val lower = fileName.lowercase()
        return lower.endsWith(".mp4") ||
            lower.endsWith(".mov") ||
            lower.endsWith(".m4v") ||
            lower.endsWith(".3gp")
    }

    /**
     * Scans top-level ISO/IEC 14496-12 boxes in < 1ms.
     * If the MP4 file has its 'moov' index box placed AFTER 'mdat' (at the end of the file,
     * as produced by video mergers/recorders without faststart), extracts the 'mdat' header
     * and the tail 'moov' box so that if the user pauses or cancels the transfer early,
     * the partial file can still be played immediately by any video player.
     */
    fun extractTailMoovMetadata(fc: FileChannel, fileSize: Long): Mp4TailMoovMetadata? {
        if (fileSize < 4L * 1024L * 1024L) return null
        try {
            val headerBuf = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
            var pos = 0L
            var ftypFound = false
            var mdatOffset = -1L
            var mdatHeaderSize = 0
            var originalMdatHeader = ByteArray(0)
            var moovOffset = -1L
            var moovSize = 0L
            var boxCount = 0

            while (pos + 8L <= fileSize && boxCount < 64) {
                boxCount++
                headerBuf.clear()
                val readBytes = fc.read(headerBuf, pos)
                if (readBytes < 8) break
                headerBuf.flip()

                val u32Size = headerBuf.int.toLong() and 0xFFFFFFFFL
                val typeBytes = ByteArray(4)
                headerBuf.get(typeBytes)
                val boxType = String(typeBytes, Charsets.US_ASCII)

                val boxSize: Long
                val hdrSize: Int
                if (u32Size == 1L) {
                    if (readBytes < 16) break
                    boxSize = headerBuf.long
                    hdrSize = 16
                } else if (u32Size == 0L) {
                    boxSize = fileSize - pos
                    hdrSize = 8
                } else {
                    boxSize = u32Size
                    hdrSize = 8
                }

                if (boxSize < hdrSize || pos + boxSize > fileSize) {
                    break
                }

                when (boxType) {
                    "ftyp" -> ftypFound = true
                    "mdat" -> {
                        if (mdatOffset < 0L) {
                            mdatOffset = pos
                            mdatHeaderSize = hdrSize
                            originalMdatHeader = ByteArray(hdrSize)
                            headerBuf.position(0)
                            headerBuf.get(originalMdatHeader, 0, hdrSize)
                        }
                    }
                    "moov" -> {
                        moovOffset = pos
                        moovSize = boxSize
                    }
                }

                pos += boxSize
            }

            // Only needed if 'moov' comes AFTER 'mdat' and is reasonably sized (<= 24 MB)
            if (ftypFound &&
                mdatOffset >= 0L &&
                moovOffset > mdatOffset &&
                moovSize in 32L..(24L * 1024L * 1024L) &&
                (moovOffset + moovSize) <= fileSize
            ) {
                val moovBytes = ByteArray(moovSize.toInt())
                val moovBuf = ByteBuffer.wrap(moovBytes)
                var totalRead = 0
                while (totalRead < moovBytes.size) {
                    val r = fc.read(moovBuf, moovOffset + totalRead)
                    if (r <= 0) break
                    totalRead += r
                }
                if (totalRead == moovBytes.size &&
                    moovBytes[4] == 'm'.code.toByte() &&
                    moovBytes[5] == 'o'.code.toByte() &&
                    moovBytes[6] == 'o'.code.toByte() &&
                    moovBytes[7] == 'v'.code.toByte()
                ) {
                    return Mp4TailMoovMetadata(
                        mdatOffset = mdatOffset,
                        mdatHeaderSize = mdatHeaderSize,
                        originalMdatHeader = originalMdatHeader,
                        tailMoovBytes = moovBytes
                    )
                }
            }
        } catch (_: Throwable) {
        }
        return null
    }

    /**
     * Patches the 'mdat' box size to match [contiguousBytesWritten] and appends the tail 'moov'
     * box right at [contiguousBytesWritten]. Because 'mdat' remains at its exact original byte
     * offset from 0, all 'stco'/'co64' chunk offsets for the downloaded portion remain 100% valid!
     */
    fun applyPartialMoovTail(
        fc: FileChannel,
        contiguousBytesWritten: Long,
        meta: Mp4TailMoovMetadata
    ): Long {
        if (contiguousBytesWritten <= meta.mdatOffset + meta.mdatHeaderSize) {
            return contiguousBytesWritten
        }
        return try {
            val newMdatSize = contiguousBytesWritten - meta.mdatOffset
            val hdrBuf = ByteBuffer.allocate(meta.mdatHeaderSize).order(ByteOrder.BIG_ENDIAN)
            if (meta.mdatHeaderSize == 16) {
                hdrBuf.putInt(1)
                hdrBuf.put('m'.code.toByte())
                hdrBuf.put('d'.code.toByte())
                hdrBuf.put('a'.code.toByte())
                hdrBuf.put('t'.code.toByte())
                hdrBuf.putLong(newMdatSize)
            } else {
                hdrBuf.putInt(newMdatSize.coerceAtMost(0xFFFFFFFFL).toInt())
                hdrBuf.put('m'.code.toByte())
                hdrBuf.put('d'.code.toByte())
                hdrBuf.put('a'.code.toByte())
                hdrBuf.put('t'.code.toByte())
            }
            hdrBuf.flip()
            while (hdrBuf.hasRemaining()) {
                fc.write(hdrBuf, meta.mdatOffset + hdrBuf.position())
            }

            val moovBuf = ByteBuffer.wrap(meta.tailMoovBytes)
            var writtenMoov = 0L
            while (moovBuf.hasRemaining()) {
                val w = fc.write(moovBuf, contiguousBytesWritten + writtenMoov)
                if (w <= 0) break
                writtenMoov += w
            }

            val finalPartialSize = contiguousBytesWritten + writtenMoov
            fc.truncate(finalPartialSize)
            fc.force(true)
            finalPartialSize
        } catch (_: Throwable) {
            contiguousBytesWritten
        }
    }

    /**
     * Restores the original 'mdat' box header if the user paused the transfer, opened the partial
     * video, and then tapped 'Devam Et' (Resume) to continue downloading the rest of the file.
     */
    fun restoreOriginalMdatForResume(
        fc: FileChannel,
        contiguousBytesWritten: Long,
        meta: Mp4TailMoovMetadata
    ) {
        try {
            val origBuf = ByteBuffer.wrap(meta.originalMdatHeader)
            while (origBuf.hasRemaining()) {
                fc.write(origBuf, meta.mdatOffset + origBuf.position())
            }
            fc.truncate(contiguousBytesWritten)
            fc.position(contiguousBytesWritten)
        } catch (_: Throwable) {
        }
    }
}
