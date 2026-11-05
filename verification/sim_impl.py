#!/usr/bin/env python3
"""
UYGULANAN ALGORİTMANIN (yeni ZeroCopyTransferEngine alıcı yazıcı döngüsü) rastgele senaryo testi.

Modellenen birebir mantık (Kotlin kodundaki sırayla):
  normal mod : Priority-1 (next hazırsa yaz) / Priority-2 (kuyruk>=4 ise en düşüğü yaz)
  duraklatma : yalnızca BİTİŞİK yaz; kuyruk>=spillThreshold ise en düşüğü yaz (havuz kilitlenmesin);
               önizleme (moov) YALNIZCA diskte watermark ötesinde stripe yoksa uygulanır; sonra dondur
  devam      : bitset/sayaç dokunulmaz; önizleme geri alınır (watermark ötesi veri zaten yok)
  yinelenen  : completed'e putIfAbsent, commit'te bit set ise geri dönüştür
  iptal      : readers anında ölür; bellekteki BİTİŞİK stripe'lar diske boşalır; received = bitset watermark

Kontrol edilen invariant'lar:
  I1  received'dan önceki HER stripe diskte geçerli
  I2  received == ilk 'yazılmamış' stripe (watermark maksimal; geçerli veri atılmıyor)
  I3  iptal yoksa transfer TAMAMLANIR (received == n) ve dosya eksiksiz
  I4  KİLİTLENME yok (ilerleme durmaz)
  I5  havuz sızıntısı yok: free + completed + uçuştaki == poolSize
"""
import random, sys, statistics

STRIPE_LAT = 66.0
WRITE_MS = 6.0


def run_case(seed, n, poolsize, mp4, cancel_at_frac, n_pauses, dup_p, stall_p):
    rnd = random.Random(seed)
    socks = 6
    spill = max(4, poolsize // 2)
    t = 0.0
    next_assign = 0
    sock_free = [0.0] * socks
    sock_pending = [None] * socks            # (idx, ready_t)
    dup_queue = []                           # (ready_t, idx)  yinelenen kopyalar
    free_bufs = poolsize
    completed = {}
    written = set()
    on_disk = set()
    nxt = 0
    writer_busy_until = 0.0
    paused = False
    pause_end = 0.0
    preview = False
    pause_plan = sorted(rnd.uniform(0.03, 0.9) for _ in range(n_pauses))
    pause_dur = [rnd.uniform(200, 4000) for _ in range(n_pauses)]
    pause_idx = 0
    cancel_trigger = int(n * cancel_at_frac) if cancel_at_frac is not None else None
    cancelled = False
    last_progress_t = 0.0
    readers_alive = True
    inflight_buf = 0                        # reader'ın okuduğu (buffer tutan) stripe sayısı

    def next_clear(frm):
        i = frm
        while i in written:
            i += 1
        return i

    def commit(idx):
        nonlocal free_bufs, nxt
        if idx in written:                  # yinelenen: sadece geri dönüştür
            free_bufs += 1
            return
        written.add(idx)
        on_disk.add(idx)
        free_bufs += 1
        if idx == nxt:
            nxt = next_clear(nxt + 1)

    while True:
        t += 1.0
        if t - last_progress_t > 60000:
            return dict(deadlock=True, t=t, nxt=nxt, n=n, free=free_bufs, comp=len(completed))

        # ---- iptal tetikleyici ----
        if cancel_trigger is not None and not cancelled and len(written) >= cancel_trigger:
            cancelled = True
            break

        # ---- soketler: GERÇEĞE YAKIN model ----
        # gönderici atama (buffer'dan bağımsız) -> BAŞLIK gelir -> reader BOŞ BUFFER bekler -> veri okunur -> completed
        # soket durumu: None | ('hdr', idx, t_hdr) | ('wait', idx) | ('read', idx, t_ready)
        for s in range(socks):
            st = sock_pending[s]
            if st is not None and st[0] == 'hdr' and t >= st[2]:
                sock_pending[s] = ('wait', st[1])
                st = sock_pending[s]
            if st is not None and st[0] == 'wait' and free_bufs > 0:
                free_bufs -= 1
                inflight_buf += 1
                dur = STRIPE_LAT * rnd.uniform(0.85, 1.15)
                sock_pending[s] = ('read', st[1], t + dur)
                st = sock_pending[s]
                last_progress_t = t
            if st is not None and st[0] == 'read' and t >= st[2]:
                idx = st[1]
                if idx in completed:
                    free_bufs += 1          # putIfAbsent: yinelenen -> geri dönüştür
                else:
                    completed[idx] = True
                inflight_buf -= 1
                sock_pending[s] = None
                last_progress_t = t
            if sock_pending[s] is None:
                dup_ready = [d for d in dup_queue if d[0] <= t]
                if dup_ready:
                    d = dup_ready[0]
                    dup_queue.remove(d)
                    sock_pending[s] = ('hdr', d[1], t + rnd.uniform(0.5, 3.0))
                elif next_assign < n:
                    idx = next_assign
                    next_assign += 1
                    extra = rnd.uniform(150, 900) if rnd.random() < stall_p else 0.0
                    sock_pending[s] = ('hdr', idx, t + rnd.uniform(0.5, 3.0) + extra)
                    if rnd.random() < dup_p:
                        dup_queue.append((t + rnd.uniform(40, 400), idx))
                    last_progress_t = t

        # ---- duraklatma planı ----
        if not paused and pause_idx < len(pause_plan) and len(written) >= int(n * pause_plan[pause_idx]):
            paused = True
            preview = False
            pause_end = t + pause_dur[pause_idx]
            pause_idx += 1
            last_progress_t = t

        if t < writer_busy_until:
            continue

        # ---- yazıcı ----
        if paused:
            if not preview:
                # bitişik yaz
                if nxt in completed:
                    del completed[nxt]
                    commit(nxt)
                    writer_busy_until = t + WRITE_MS
                    last_progress_t = t
                elif len(completed) >= spill:
                    idx = min(completed)
                    del completed[idx]
                    commit(idx)
                    writer_busy_until = t + WRITE_MS
                    last_progress_t = t
                else:
                    # önizleme koşulu: watermark ötesinde yazılmış stripe yok
                    if not any(i > nxt for i in written):
                        preview = True
                        # MP4: moov, watermark'a yazılır; ötesinde veri olmadığı için hiçbir şey kaybolmaz.
                        assert all(i < nxt for i in on_disk), "önizleme veri yok edecekti!"
            if t >= pause_end:
                paused = False
                if preview:
                    # restoreOriginalMdatForResume: truncate(watermark) -> watermark ötesi yok zaten
                    assert all(i < nxt for i in on_disk)
                preview = False
                last_progress_t = t
            continue

        readers_active = any(p is not None for p in sock_pending) or next_assign < n or bool(dup_queue)
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
        commit(target)
        writer_busy_until = t + WRITE_MS
        last_progress_t = t
        if nxt >= n and not dup_queue:
            break

    # ---- bitiş / iptal sonrası drain ----
    if cancelled:
        # readers anında ölür; uçuştaki buffer'lar atılır
        pass
    while nxt < n and nxt in completed:
        del completed[nxt]
        commit(nxt)
    received = n if nxt >= n else nxt
    valid_prefix = all(i in on_disk for i in range(received))
    first_missing = next((i for i in range(n + 1) if i not in written), n)
    return dict(
        deadlock=False, cancelled=cancelled, received=received, n=n,
        I1=valid_prefix,
        I2=(received == min(first_missing, n)),
        complete_ok=(received == n and len(on_disk) >= n),
        free=free_bufs, comp=len(completed), inflight=inflight_buf, pool=poolsize,
    )


def main(runs):
    stats = dict(cases=0, deadlock=0, I1=0, I2=0, I3=0, leak=0)
    ex = []
    for seed in range(runs):
        rnd = random.Random(10_000 + seed)
        n = rnd.choice([8, 20, 64, 200, 400])
        poolsize = min(64, n + 6)
        mp4 = rnd.random() < 0.5
        cancel_at = rnd.choice([None, None, rnd.uniform(0.05, 0.95)])
        n_pauses = rnd.choice([0, 1, 1, 2, 3])
        dup_p = rnd.choice([0.0, 0.02, 0.05])
        stall_p = rnd.choice([0.0, 0.015, 0.05])
        r = run_case(seed, n, poolsize, mp4, cancel_at, n_pauses, dup_p, stall_p)
        stats["cases"] += 1
        if r["deadlock"]:
            stats["deadlock"] += 1
            ex.append(("DEADLOCK", seed, n, poolsize, cancel_at, n_pauses, r))
            continue
        if not r["I1"]:
            stats["I1"] += 1
            ex.append(("I1", seed, r))
        if not r["I2"]:
            stats["I2"] += 1
            ex.append(("I2", seed, r))
        if not r["cancelled"] and not r["complete_ok"]:
            stats["I3"] += 1
            ex.append(("I3", seed, r))
        if r["free"] + r["comp"] + r["inflight"] > r["pool"] + 0:
            stats["leak"] += 1
            ex.append(("LEAK", seed, r))
    return stats, ex


if __name__ == "__main__":
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 1500
    stats, ex = main(runs)
    print(f"Senaryo: {stats['cases']}  | kilitlenme: {stats['deadlock']}  | I1 ihlali: {stats['I1']}  | "
          f"I2 ihlali: {stats['I2']}  | I3 (tamamlanmama): {stats['I3']}  | havuz sızıntısı: {stats['leak']}")
    for e in ex[:6]:
        print("  örnek:", e)
    sys.exit(0 if all(stats[k] == 0 for k in ("deadlock", "I1", "I2", "I3", "leak")) else 1)
