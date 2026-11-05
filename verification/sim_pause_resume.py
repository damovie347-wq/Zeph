#!/usr/bin/env python3
"""
v12 ALICI MUHASEBESİ SİMÜLASYONU  (ZeroCopyTransferEngine.handleIncomingClientChannel, satır 823-988)

Modellenen birebir mantık:
  * 6 soket, gönderici iş-çalma (nextStripeIdx.getAndIncrement), 1 MB stripe
  * Alıcı: completedStripes (SkipListMap), tek yazıcı thread, writtenStripesBitSet, nextContiguousStripe
  * Priority-1: next stripe hazırsa yaz.  Priority-2 (Anti-HoL): kuyrukta >=4 stripe varsa EN DÜŞÜĞÜ yaz
  * Duraklat: 180 ms drain -> watermark -> (MP4) applyPartialMoovTail = truncate(watermark+moov)
  * Devam   : bitset.clear(next, length); totalBytesWritten = watermark; (MP4) truncate(watermark)
  * Bitiş   : received = expected  EĞER totalBytesWritten >= expected, DEĞİLSE contiguousBytes ; dosya o boyuta TRUNCATE
Gecikme modeli: stripe başına ±15% jitter + %1.5 olasılıkla 150-700 ms duraklama (Wi-Fi RTO/yeniden iletim).
"""
import random, sys, statistics

STRIPE = 1  # birim = 1 stripe (1 MiB)


def simulate(policy, n_stripes, pause_at_frac, pause_ms, seed, mp4=False, stall_p=0.015):
    rnd = random.Random(seed)
    socks = 6
    base_ms = 66.0            # 6 soket toplam ~90 MB/s  => stripe/soket ~66 ms
    write_ms = 6.0            # tek yazıcı: 1 MiB pwrite ~6 ms
    t = 0.0
    next_assign = 0
    sock_free = [0.0] * socks
    sock_pending = [None] * socks      # (idx, ready_time)
    completed = {}                      # idx -> True (bellekte hazır stripe)
    written = set()                     # writtenStripesBitSet (policy'ye göre bozulabilir)
    on_disk = set()                     # diskte GERÇEKTEN geçerli veri olan stripe'lar
    nxt = 0                             # nextContiguousStripe
    total_written = 0                   # totalBytesWrittenAtomic (stripe birimi)
    paused = False
    pause_started = None
    resumed = False
    pause_applied = False
    writer_busy_until = 0.0
    pause_trigger = int(n_stripes * pause_at_frac)
    pool_cap = 64
    watermark = 0

    def next_clear(frm):
        i = frm
        while i in written:
            i += 1
        return i

    while True:
        t += 1.0
        # --- soketler: stripe tamamla / yeni stripe al ---
        for s in range(socks):
            p = sock_pending[s]
            if p is not None and t >= p[1] and len(completed) < pool_cap:
                completed[p[0]] = True
                sock_pending[s] = None
            if sock_pending[s] is None and t >= sock_free[s] and next_assign < n_stripes:
                idx = next_assign
                next_assign += 1
                dur = base_ms * rnd.uniform(0.85, 1.15)
                if rnd.random() < stall_p:
                    dur += rnd.uniform(150, 700)
                sock_free[s] = t + dur
                sock_pending[s] = (idx, t + dur)

        # --- duraklatma tetikleyici (kullanıcı ~%5-10'da durdurur) ---
        if not paused and not resumed and total_written >= pause_trigger:
            paused = True
            pause_started = t
            pause_applied = False

        # --- yazıcı thread ---
        if t < writer_busy_until:
            continue

        if paused:
            if not pause_applied:
                # 180 ms drain: en düşük hazır stripe'ı yaz (pollFirstEntry), yoksa 15 ms uyu
                deadline = t + 180
                tt = t
                while tt < deadline:
                    # drain sırasında soketler de teslim etmeye devam eder
                    for s in range(socks):
                        p = sock_pending[s]
                        if p is not None and tt >= p[1] and len(completed) < pool_cap:
                            completed[p[0]] = True
                            sock_pending[s] = None
                        if sock_pending[s] is None and tt >= sock_free[s] and next_assign < n_stripes:
                            idx = next_assign
                            next_assign += 1
                            dur = base_ms * rnd.uniform(0.85, 1.15)
                            if rnd.random() < stall_p:
                                dur += rnd.uniform(150, 700)
                            sock_free[s] = tt + dur
                            sock_pending[s] = (idx, tt + dur)
                    if completed:
                        sidx = min(completed)
                        del completed[sidx]
                        written.add(sidx); on_disk.add(sidx); total_written += 1
                        if sidx == nxt:
                            nxt = next_clear(nxt + 1)
                        tt += write_ms
                    else:
                        tt += 15
                watermark = min(nxt, n_stripes)
                if mp4 and 0 < watermark < n_stripes:
                    if policy == "v12":
                        moov = 6   # ~6 MiB moov kuyruğu: applyPartialMoovTail -> truncate(watermark + moov)
                        # watermark+moov'dan sonrası YOK EDİLİR, [watermark, watermark+moov) üstüne moov yazılır
                        for i in list(on_disk):
                            if i >= watermark:
                                on_disk.discard(i)
                    # fixed: stash + restore -> veri kaybı yok
                t = tt
                pause_applied = True
                pause_started = t
            # duraklatma süresi dolana kadar bekle (stripe'lar completed'e birikir, yazılmaz)
            if t - pause_started >= pause_ms:
                paused = False
                resumed = True
                # ---- DEVAM ET ----
                if policy == "v12":
                    # bitset.clear(next, length); totalBytesWritten = watermark; (MP4) truncate(watermark)
                    for i in [i for i in written if i >= nxt]:
                        written.discard(i)
                    total_written = watermark
                    if mp4:
                        for i in list(on_disk):
                            if i >= watermark:
                                on_disk.discard(i)
                # fixed: HİÇBİR ŞEY silinmez/sıfırlanmaz
            continue

        # --- normal yazma: Priority-1 / Priority-2 ---
        readers_active = any(p is not None for p in sock_pending) or next_assign < n_stripes
        target = nxt
        ready = target in completed
        if ready:
            del completed[target]
        elif len(completed) >= 4 or (not readers_active and completed):
            if completed:
                target = min(completed)
                del completed[target]
                ready = True
        if not ready:
            if not readers_active and not completed:
                break
            continue
        written.add(target); on_disk.add(target); total_written += 1
        if target == nxt:
            nxt = next_clear(nxt + 1)
        writer_busy_until = t + write_ms
        if nxt >= n_stripes:
            break

    # --- bitiş: yazıcı döngüsü sonrası drain + finally mantığı ---
    while nxt < n_stripes and nxt in completed:
        del completed[nxt]
        written.add(nxt); on_disk.add(nxt); total_written += 1
        nxt = next_clear(nxt + 1)
    contiguous = min(nxt, n_stripes)
    if policy == "fixed":
        # düzeltilmiş: bitset tek doğruluk kaynağı
        total_written = len(written)
    received = n_stripes if total_written >= n_stripes else contiguous
    # truncate(received): received sonrası diskte veri kalmaz
    valid_after = {i for i in on_disk if i < received}
    # dosya gerçekten eksiksiz mi?
    complete_on_disk = (received == n_stripes) and all(i in valid_after for i in range(n_stripes))
    return received, complete_on_disk, total_written


def run(n_stripes, pause_frac, pause_ms, runs, mp4, label):
    out = {}
    for policy in ("v12", "fixed"):
        bad = 0
        recs = []
        for seed in range(runs):
            rec, ok, tw = simulate(policy, n_stripes, pause_frac, pause_ms, seed, mp4=mp4)
            if not ok:
                bad += 1
                recs.append(rec)
        out[policy] = (bad, recs)
    print(f"\n=== {label} ===  dosya={n_stripes} MiB, duraklat=%{int(pause_frac*100)}'te, bekleme={pause_ms/1000:.0f}s, {runs} koşu")
    for policy in ("v12", "fixed"):
        bad, recs = out[policy]
        extra = ""
        if recs:
            extra = f"  | bozuk koşularda alıcıda 'korunan' ort={statistics.mean(recs):.0f} MiB, min={min(recs)}, maks={max(recs)}"
        print(f"  {policy:6s}: tamamlanmış transferin bozulduğu koşu = {bad:3d}/{runs}  (%{100*bad/runs:.0f}){extra}")
    return out


if __name__ == "__main__":
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    run(512, 0.05, 3000, runs, mp4=False, label="Genel dosya (zip/apk/vb.)  - %5'te duraklat/devam")
    run(512, 0.10, 3000, runs, mp4=False, label="Genel dosya               - %10'da duraklat/devam")
    run(512, 0.05, 3000, runs, mp4=True,  label="MP4 video (moov kuyruğu)  - %5'te duraklat/devam")
    run(512, 0.10, 3000, runs, mp4=True,  label="MP4 video (moov kuyruğu)  - %10'da duraklat/devam")
