// ===================================================================================================
// HARNESS: ZeroCopyTransferEngine'in GERÇEK kod dilimlerini (build_harness.py ile dosyadan kesilir) JVM'de çalıştırır.
// Android'e özgü yalnızca Process/Os/Mp4/TransferProgress için ince uyumluluk katmanı vardır.
// ===================================================================================================
import java.io.File
import java.io.FileDescriptor
import java.io.RandomAccessFile
import java.net.InetAddress
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.channels.FileChannel
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.security.MessageDigest
import java.util.BitSet
import java.util.Random
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

const val STRIPE_BYTES = 1 * 1024 * 1024
const val REORDER_RING_BUFFERS = 64
const val PARALLEL_DATA_STREAMS = 6

object Process {
    const val THREAD_PRIORITY_URGENT_AUDIO = -19
    @Suppress("UNUSED_PARAMETER")
    fun setThreadPriority(p: Int) {}
}

enum class TransferStatus { IDLE, CONNECTING, WAITING_APPROVAL, TRANSFERRING, PAUSED, COMPLETED, PARTIAL_PRESERVED, REJECTED, ERROR }

data class TransferProgress(
    val fileName: String, val fileIndex: Int = 0, val totalFiles: Int = 1, val bytesTransferred: Long, val totalBytes: Long,
    val speedMbps: Double, val etaSeconds: Long, val elapsedMillis: Long = 0L, val status: TransferStatus,
    val transportLabel: String, val isSending: Boolean = false, val savedFilePath: String? = null, val message: String? = null,
    val previewReady: Boolean = true
)

class Mp4Meta
object Mp4FastStartHelper {
    fun applyPartialMoovTail(fc: FileChannel, c: Long, m: Mp4Meta): Long = c
    fun restoreOriginalMdatForResume(fc: FileChannel, c: Long, m: Mp4Meta) {}
}

class Item(val name: String, val file: File)
class IndependentFileHandle(val fd: FileDescriptor, val channel: FileChannel, private val raf: RandomAccessFile) {
    fun closeAll() { try { raf.close() } catch (_: Throwable) {} }
}

fun openIndependentFileHandle(item: Item): IndependentFileHandle? {
    val raf = RandomAccessFile(item.file, "r")
    return IndependentFileHandle(raf.fd, raf.channel, raf)
}

@Volatile var readFaultProb = 0.0
val faultRnd = Random(1234)

fun readFullyFromChannel(ch: SocketChannel, buf: ByteBuffer): Boolean {
    while (buf.hasRemaining()) {
        val r = ch.read(buf)
        if (r < 0) return false
    }
    return true
}

fun writeFullyToChannel(ch: SocketChannel, buf: ByteBuffer): Boolean {
    while (buf.hasRemaining()) {
        val w = ch.write(buf)
        if (w < 0) return false
    }
    return true
}

fun writeBufferAtOffsetSingleThread(fd: FileDescriptor, fc: FileChannel, buf: ByteBuffer, offset: Long): Long {
    var writtenTotal = 0L
    while (buf.hasRemaining()) {
        val w = fc.write(buf, offset + writtenTotal)
        if (w <= 0) break
        writtenTotal += w.toLong()
    }
    return writtenTotal
}

fun readBufferAtOffsetLockless(fd: FileDescriptor, fc: FileChannel, buf: ByteBuffer, offset: Long): Int {
    if (readFaultProb > 0.0 && synchronized(faultRnd) { faultRnd.nextDouble() } < readFaultProb) return 0 // enjekte okuma hatası
    var readTotal = 0
    while (buf.hasRemaining()) {
        val r = fc.read(buf, offset + readTotal)
        if (r <= 0) break
        readTotal += r
    }
    return readTotal
}

fun computeSustainedSpeedMbps(samples: ConcurrentLinkedDeque<Pair<Long, Long>>, nowNs: Long, totalBytes: Long, startTimeNs: Long): Double {
    samples.addLast(nowNs to totalBytes)
    val windowNs = 2_500_000_000L
    while (samples.size > 2) {
        val first = samples.peekFirst() ?: break
        if (nowNs - first.first > windowNs) samples.pollFirst() else break
    }
    val totalElapsedSec = ((nowNs - startTimeNs) / 1_000_000_000.0).coerceAtLeast(0.05)
    return (totalBytes / (1024.0 * 1024.0)) / totalElapsedSec
}

// ---------------------------------------------------------------------------------------------------
// GÖNDERİCİ (gerçek dilim)
// ---------------------------------------------------------------------------------------------------
fun runSenderNew(workerChannels: List<SocketChannel>, srcFile: File, sessionCancel: AtomicBoolean, isPaused: AtomicBoolean, confirmedWorkerCount: Int): Long {
    val item = Item(srcFile.name, srcFile)
    val items = listOf(item)
    val fileIdx = 0
    val actualSize = srcFile.length()
    val primaryHandle: IndependentFileHandle? = openIndependentFileHandle(item)
    var transportLabel = "test"
    val totalBytesSentAtomic = AtomicLong(0L)
    val startTimeNs = System.nanoTime()
    val startWallMs = System.currentTimeMillis()
    val lastUiUpdateMs = AtomicLong(0L)
    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
    val diagReadNs = AtomicLong(0L)
    val diagSockNs = AtomicLong(0L)
    var diagWorkers = 1
    val onProgressUpdate: (TransferProgress) -> Unit = { }
    val totalStripes = ((actualSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
    if (primaryHandle != null) {
/*__TX_NEW__*/
    }
    primaryHandle?.closeAll()
    return totalBytesSentAtomic.get()
}

fun runSenderV12(workerChannels: List<SocketChannel>, srcFile: File, sessionCancel: AtomicBoolean, isPaused: AtomicBoolean, confirmedWorkerCount: Int): Long {
    val item = Item(srcFile.name, srcFile)
    val items = listOf(item)
    val fileIdx = 0
    val actualSize = srcFile.length()
    val primaryHandle: IndependentFileHandle? = openIndependentFileHandle(item)
    var transportLabel = "test"
    val totalBytesSentAtomic = AtomicLong(0L)
    val startTimeNs = System.nanoTime()
    val startWallMs = System.currentTimeMillis()
    val lastUiUpdateMs = AtomicLong(0L)
    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
    val onProgressUpdate: (TransferProgress) -> Unit = { }
    val totalStripes = ((actualSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()
    if (primaryHandle != null) {
/*__TX_V12__*/
    }
    primaryHandle?.closeAll()
    return totalBytesSentAtomic.get()
}

// ---------------------------------------------------------------------------------------------------
// ALICI (gerçek dilim): yeni ve v12
// ---------------------------------------------------------------------------------------------------
class RxResult(val totalBytesReceived: Long, val contiguousBytes: Long, val writtenBytes: Long)

fun runReceiverNew(workerChannels: List<SocketChannel>, dest: File, expectedFileSize: Long, sessionCancel: AtomicBoolean, isPaused: AtomicBoolean): RxResult {
    val targetFile = dest
    val fileIdx = 0
    val fileCount = 1
    val transportLabel = "test"
    val startTimeNs = System.nanoTime()
    val startWallMs = System.currentTimeMillis()
    var lastUiUpdateMs = 0L
    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
    val diagWriteNs = AtomicLong(0L)
    val diagWriteBytes = AtomicLong(0L)
    val diagPoolWaitNs = AtomicLong(0L)
    val diagWriterIdleNs = AtomicLong(0L)
    var receiveFailure: Throwable? = null
    val contiguousWatermarkBytes = AtomicLong(0L)
    val totalBytesWrittenAtomic = AtomicLong(0L)
    val onProgressUpdate: (TransferProgress) -> Unit = { }
    val mp4Meta: Mp4Meta? = null
    var raf: RandomAccessFile? = null
    var fileChannel: FileChannel? = null
    try {
        raf = RandomAccessFile(targetFile, "rw")
        val rawFd = raf.fd
        fileChannel = raf.channel
        fileChannel.position(0L)
        if (workerChannels.isNotEmpty() && expectedFileSize > 0L) {
            run {
/*__RX_NEW__*/
            }
        }
    } catch (t: Throwable) {
        receiveFailure = t
    }
    val contiguousBytes = contiguousWatermarkBytes.get().coerceAtMost(expectedFileSize)
    val totalBytesReceived = if (totalBytesWrittenAtomic.get() >= expectedFileSize) expectedFileSize else contiguousBytes
    try { fileChannel?.truncate(totalBytesReceived) } catch (_: Throwable) {}
    try { fileChannel?.close() } catch (_: Throwable) {}
    try { raf?.close() } catch (_: Throwable) {}
    return RxResult(totalBytesReceived, contiguousBytes, totalBytesWrittenAtomic.get())
}

fun runReceiverV12(workerChannels: List<SocketChannel>, dest: File, expectedFileSize: Long, sessionCancel: AtomicBoolean, isPaused: AtomicBoolean): RxResult {
    val targetFile = dest
    val fileIdx = 0
    val fileCount = 1
    val transportLabel = "test"
    val startTimeNs = System.nanoTime()
    val startWallMs = System.currentTimeMillis()
    var lastUiUpdateMs = 0L
    val speedSamples = ConcurrentLinkedDeque<Pair<Long, Long>>()
    val contiguousWatermarkBytes = AtomicLong(0L)
    val totalBytesWrittenAtomic = AtomicLong(0L)
    val onProgressUpdate: (TransferProgress) -> Unit = { }
    val mp4Meta: Mp4Meta? = null
    var raf: RandomAccessFile? = null
    var fileChannel: FileChannel? = null
    try {
        raf = RandomAccessFile(targetFile, "rw")
        val rawFd = raf.fd
        fileChannel = raf.channel
        fileChannel.position(0L)
        if (workerChannels.isNotEmpty() && expectedFileSize > 0L) {
            run {
/*__RX_V12__*/
            }
        }
    } catch (_: Throwable) {
    }
    val contiguousBytes = contiguousWatermarkBytes.get().coerceAtMost(expectedFileSize)
    val totalBytesReceived = if (totalBytesWrittenAtomic.get() >= expectedFileSize) expectedFileSize else contiguousBytes
    try { fileChannel?.truncate(totalBytesReceived) } catch (_: Throwable) {}
    try { fileChannel?.close() } catch (_: Throwable) {}
    try { raf?.close() } catch (_: Throwable) {}
    return RxResult(totalBytesReceived, contiguousBytes, totalBytesWrittenAtomic.get())
}

// ---------------------------------------------------------------------------------------------------
// "Rogue" gönderici: Wi-Fi benzeri jitter + durmalar + yinelenen stripe (alıcı mantığını sınamak için)
// ---------------------------------------------------------------------------------------------------
fun runRogueSender(chans: List<SocketChannel>, src: File, cancel: AtomicBoolean, jitterMs: Int, stallP: Double, dupP: Double, seed: Long) {
    val size = src.length()
    val total = ((size + STRIPE_BYTES - 1L) / STRIPE_BYTES).toInt()
    val next = AtomicInteger(0)
    val threads = chans.mapIndexed { ci, ch ->
        Thread {
            val rnd = Random(seed * 31 + ci)
            val raf = RandomAccessFile(src, "r")
            val fc = raf.channel
            val buf = ByteBuffer.allocateDirect(STRIPE_BYTES)
            val hdr = ByteBuffer.allocateDirect(8).order(ByteOrder.BIG_ENDIAN)
            val pendingDup = ArrayList<Int>()
            fun sendStripe(idx: Int) {
                val off = idx.toLong() * STRIPE_BYTES
                val len = (size - off).coerceAtMost(STRIPE_BYTES.toLong()).toInt()
                buf.clear(); buf.limit(len)
                var rd = 0
                while (buf.hasRemaining()) { val r = fc.read(buf, off + rd); if (r <= 0) break; rd += r }
                buf.flip()
                hdr.clear(); hdr.putInt(idx); hdr.putInt(len); hdr.flip()
                val g = arrayOf(hdr, buf)
                while (hdr.hasRemaining() || buf.hasRemaining()) { if (ch.write(g) < 0L) break }
            }
            try {
                while (!cancel.get()) {
                    val idx = next.getAndIncrement()
                    if (idx >= total) break
                    if (stallP > 0 && rnd.nextDouble() < stallP) Thread.sleep(150L + rnd.nextInt(550))
                    else if (jitterMs > 0) Thread.sleep(rnd.nextInt(jitterMs + 1).toLong())
                    sendStripe(idx)
                    if (dupP > 0 && rnd.nextDouble() < dupP) pendingDup.add(idx)
                    if (pendingDup.isNotEmpty() && rnd.nextDouble() < 0.3) sendStripe(pendingDup.removeAt(rnd.nextInt(pendingDup.size)))
                }
                for (d in pendingDup) sendStripe(d)
                hdr.clear(); hdr.putInt(-1); hdr.putInt(0); hdr.flip()
                while (hdr.hasRemaining()) { if (ch.write(hdr) < 0) break }
            } catch (_: Throwable) {
            } finally {
                try { raf.close() } catch (_: Throwable) {}
            }
        }.apply { start() }
    }
    for (t in threads) t.join()
}

// ---------------------------------------------------------------------------------------------------
// Test altyapısı
// ---------------------------------------------------------------------------------------------------
val srcCache = HashMap<String, File>()
fun makeSource(size: Long, seed: Long): File {
    val key = "$size-$seed"
    srcCache[key]?.let { return it }
    val f = File("/tmp/zeph_src_$key.bin")
    if (!f.exists() || f.length() != size) {
        val rnd = Random(seed)
        val chunk = ByteArray(1 shl 20)
        RandomAccessFile(f, "rw").use { raf ->
            raf.setLength(0)
            var left = size
            while (left > 0) {
                rnd.nextBytes(chunk)
                val n = minOf(left, chunk.size.toLong()).toInt()
                raf.write(chunk, 0, n)
                left -= n
            }
        }
    }
    srcCache[key] = f
    return f
}

fun sha256(f: File, limit: Long = f.length()): String {
    val md = MessageDigest.getInstance("SHA-256")
    f.inputStream().buffered(1 shl 20).use { ins ->
        val b = ByteArray(1 shl 20)
        var left = limit
        while (left > 0) {
            val n = ins.read(b, 0, minOf(left, b.size.toLong()).toInt())
            if (n <= 0) break
            md.update(b, 0, n)
            left -= n
        }
    }
    return md.digest().joinToString("") { "%02x".format(it) }
}

class SockPair(val tx: List<SocketChannel>, val rx: List<SocketChannel>)

fun connectSockets(n: Int): SockPair {
    val server = ServerSocketChannel.open()
    server.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
    val addr = server.localAddress
    val tx = ArrayList<SocketChannel>()
    val rx = ArrayList<SocketChannel>()
    for (i in 0 until n) {
        val c = SocketChannel.open()
        c.connect(addr)
        val a = server.accept()
        for (ch in listOf(c, a)) {
            ch.socket().tcpNoDelay = true
            ch.socket().sendBufferSize = 4 * 1024 * 1024
            ch.socket().receiveBufferSize = 16 * 1024 * 1024
            ch.configureBlocking(true)
        }
        tx.add(c); rx.add(a)
    }
    server.close()
    return SockPair(tx, rx)
}

fun rstClose(ch: SocketChannel) {
    try { ch.socket().setSoLinger(true, 0) } catch (_: Throwable) {}
    try { ch.close() } catch (_: Throwable) {}
}

class Opts(
    val size: Long,
    val tx: String = "new",
    val rx: String = "new",
    val readFault: Double = 0.0,
    val rxPause: Boolean = false,
    val txPause: Boolean = false,
    val killGraceful: Boolean = false,
    val rogueStallP: Double = 0.0,
    val rogueDupP: Double = 0.0,
    val rogueJitter: Int = 0,
    val cancelAfterMs: Long = -1L,
    val seed: Long = 1L
)

class Outcome(val ok: Boolean, val detail: String, val mbps: Double, val received: Long, val expected: Long, val hung: Boolean = false)

fun runCase(o: Opts): Outcome {
    val src = makeSource(o.size, 7L)
    val dest = File.createTempFile("zeph_dst_", ".bin")
    dest.deleteOnExit()
    val sp = connectSockets(PARALLEL_DATA_STREAMS)
    val rxCancel = AtomicBoolean(false); val rxPaused = AtomicBoolean(false)
    val txCancel = AtomicBoolean(false); val txPaused = AtomicBoolean(false)
    readFaultProb = o.readFault
    val done = AtomicBoolean(false)
    var rxRes: RxResult? = null
    val t0 = System.nanoTime()

    val rxThread = Thread {
        rxRes = if (o.rx == "v12") runReceiverV12(sp.rx, dest, o.size, rxCancel, rxPaused) else runReceiverNew(sp.rx, dest, o.size, rxCancel, rxPaused)
    }
    val txThread = Thread {
        try {
            when (o.tx) {
                "v12" -> runSenderV12(sp.tx, src, txCancel, txPaused, sp.tx.size)
                "rogue" -> runRogueSender(sp.tx, src, txCancel, o.rogueJitter, o.rogueStallP, o.rogueDupP, o.seed)
                else -> runSenderNew(sp.tx, src, txCancel, txPaused, sp.tx.size)
            }
        } finally {
            if (o.cancelAfterMs < 0) sp.tx.forEach { try { it.close() } catch (_: Throwable) {} }
        }
    }
    rxThread.start(); txThread.start()

    fun toggler(flag: AtomicBoolean, seedOff: Long) = Thread {
        val rnd = Random(o.seed * 7 + seedOff)
        try {
            while (!done.get()) {
                Thread.sleep(40L + rnd.nextInt(200))
                if (done.get()) break
                flag.set(true)
                Thread.sleep(60L + rnd.nextInt(300))
                flag.set(false)
            }
        } catch (_: Throwable) {}
        flag.set(false)
    }.apply { isDaemon = true }
    if (o.rxPause) toggler(rxPaused, 1).start()
    if (o.txPause) toggler(txPaused, 2).start()
    if (o.killGraceful) Thread {
        try { Thread.sleep(15L + Random(o.seed).nextInt(60)); sp.tx[2].close() } catch (_: Throwable) {}
    }.apply { isDaemon = true; start() }
    if (o.cancelAfterMs >= 0) Thread {
        try {
            Thread.sleep(o.cancelAfterMs)
            // cancelAndPreservePartialFile(): bayrak + veri soketlerini RST ile kes
            rxCancel.set(true); txCancel.set(true)
            sp.rx.forEach { rstClose(it) }
            sp.tx.forEach { rstClose(it) }
        } catch (_: Throwable) {}
    }.apply { isDaemon = true; start() }

    rxThread.join(90_000); txThread.join(30_000)
    val hung = rxThread.isAlive || txThread.isAlive
    done.set(true)
    val secs = (System.nanoTime() - t0) / 1e9
    if (hung) {
        sp.rx.forEach { rstClose(it) }; sp.tx.forEach { rstClose(it) }
        return Outcome(false, "ASILI KALDI (kilitlenme?)", 0.0, 0, o.size, true)
    }
    val r = rxRes!!
    val prefixHashOk = sha256(dest, r.totalBytesReceived) == sha256(src, r.totalBytesReceived) && dest.length() == r.totalBytesReceived
    dest.delete()
    val complete = r.totalBytesReceived == o.size
    val mbps = (o.size / 1048576.0) / secs
    return if (o.cancelAfterMs >= 0) {
        Outcome(prefixHashOk, "iptal: korunan=${r.totalBytesReceived / 1048576.0}MiB bitişik=${r.contiguousBytes / 1048576.0} yazılan=${r.writtenBytes / 1048576.0} önek-bayt-bayt-eşit=$prefixHashOk", mbps, r.totalBytesReceived, o.size)
    } else {
        Outcome(complete && prefixHashOk, "tamam=$complete korunan=${r.totalBytesReceived} / ${o.size} hash-eşit=$prefixHashOk", mbps, r.totalBytesReceived, o.size)
    }
}

var failures = 0
fun report(name: String, o: Outcome) {
    val tag = if (o.ok) "GEÇTİ " else "KALDI "
    if (!o.ok) failures++
    println("  [$tag] $name  -> ${o.detail}")
}

fun main(args: Array<String>) {
    val mode = if (args.isNotEmpty()) args[0] else "all"
    val MB = 1024L * 1024L

    if (mode == "all" || mode == "correct") {
        println("== T1: temiz transfer + sınır boyutları (gerçek gönderici + gerçek alıcı) ==")
        for (sz in longArrayOf(1 * MB, 1 * MB + 1, 4 * MB, 5 * MB + 123, 6 * MB, 7 * MB + 123, 37 * MB + 999, 64 * MB + 1, 200 * MB + 12345))
            report("boyut=$sz", runCase(Opts(sz)))
        println("== T2: okuma hataları enjekte (pread kısa dönüyor) -> yeniden kuyruk ==")
        for (p in doubleArrayOf(0.02, 0.10))
            report("readFault=$p", runCase(Opts(96 * MB + 7, readFault = p)))
        println("== T3: bir soket nazikçe kapatılıyor (worker ölür, stripe yeniden kuyruğa) ==")
        for (seed in 1L..6L)
            report("seed=$seed", runCase(Opts(160 * MB + 3, killGraceful = true, seed = seed)))
        println("== T4: alıcı duraklat/devam (gerçek alıcı) ==")
        for (seed in 1L..6L)
            report("seed=$seed", runCase(Opts(160 * MB + 3, rxPause = true, seed = seed)))
        println("== T5: gönderici duraklat/devam ==")
        for (seed in 1L..4L)
            report("seed=$seed", runCase(Opts(160 * MB + 3, txPause = true, seed = seed)))
        println("== T6: hepsi birden (okuma hatası + soket kaybı + iki taraflı duraklat) ==")
        for (seed in 1L..6L)
            report("seed=$seed", runCase(Opts(192 * MB + 17, readFault = 0.03, killGraceful = true, rxPause = true, txPause = true, seed = seed)))
        println("== T7: Wi-Fi benzeri jitter + durmalar + YİNELENEN stripe + alıcı duraklat/devam (rogue gönderici) ==")
        for (seed in 1L..8L)
            report("seed=$seed", runCase(Opts(120 * MB + 5, tx = "rogue", rogueJitter = 3, rogueStallP = 0.02, rogueDupP = 0.05, rxPause = true, seed = seed)))
        println("== T8: iptal (RST) rastgele anlarda -> korunan önek bayt bayt doğru mu? ==")
        for (seed in 1L..10L)
            report("seed=$seed", runCase(Opts(256 * MB, cancelAfterMs = 20L + seed * 23, rxPause = (seed % 2L == 0L), seed = seed)))
    }

    if (mode == "cancel") {
        println("== T8b: Wi-Fi benzeri yavaş akışta iptal (rogue gönderici) -> korunan önek bayt bayt doğru mu? ==")
        for (seed in 1L..16L)
            report("seed=$seed", runCase(Opts(120 * MB + 5, tx = "rogue", rogueJitter = 3, rogueStallP = 0.02, cancelAfterMs = 150L + seed * 120L, rxPause = (seed % 3L == 0L), seed = seed)))
    }

    if (mode == "all" || mode == "v12bug") {
        println("== v12 HATASININ GERÇEK KODDA TEKRARI: jitter + duraklat/devam (aynı rogue gönderici, aynı tohumlar) ==")
        var bad12 = 0; var badNew = 0; val N = 20
        for (seed in 1L..N.toLong()) {
            val a = runCase(Opts(120 * MB + 5, tx = "rogue", rx = "v12", rogueJitter = 3, rogueStallP = 0.02, rxPause = true, seed = seed))
            val b = runCase(Opts(120 * MB + 5, tx = "rogue", rx = "new", rogueJitter = 3, rogueStallP = 0.02, rxPause = true, seed = seed))
            if (!a.ok) bad12++
            if (!b.ok) badNew++
            if (!a.ok) println("   v12  seed=$seed: ${a.detail}")
        }
        println("  SONUÇ: v12 alıcı bozulan/eksik kalan transfer = $bad12/$N   |   yeni alıcı = $badNew/$N")
        if (badNew != 0) failures++
    }

    if (mode == "all" || mode == "bench") {
        println("== loopback JVM mikro-kıyas (Wi-Fi hızını TEMSİL ETMEZ; yalnızca yeni worker'ın v12'ye göre ek yük getirip getirmediği) ==")
        val size = 512 * MB
        fun med(l: List<Double>) = l.sorted()[l.size / 2]
        val v12s = ArrayList<Double>(); val news = ArrayList<Double>()
        runCase(Opts(size, tx = "v12")); runCase(Opts(size, tx = "new")) // ısınma
        for (i in 1..5) {
            v12s.add(runCase(Opts(size, tx = "v12", seed = 1)).mbps)
            news.add(runCase(Opts(size, tx = "new", seed = 1)).mbps)
        }
        println("  v12 gönderici  : ${v12s.joinToString { "%.0f".format(it) }} MiB/s  (medyan ${"%.0f".format(med(v12s))})")
        println("  yeni gönderici : ${news.joinToString { "%.0f".format(it) }} MiB/s  (medyan ${"%.0f".format(med(news))})")
    }

    println(if (failures == 0) "\nTÜM HARNESS TESTLERİ GEÇTİ" else "\n!!! $failures TEST KALDI !!!")
    System.exit(if (failures == 0) 0 else 1)
}
