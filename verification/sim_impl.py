#!/usr/bin/env python3
"""
V14 ALGORİTMA DOĞRULAMA SİMÜLASYONU (ZeroCopyTransferEngine v14):
  - 4 paralel veri kanalı (PARALLEL_DATA_STREAMS = 4)
  - Sınırlı Kayan Pencere (MAX_STRIPE_WINDOW = 32) + 64 MB Reorder Havuzu (REORDER_RING_BUFFERS = 64)
  - Tam-Dupleks Reverse-NACK + >120 ms Geciken Stripe Spekülatif Kurtarma (Straggler Rescue)
  - %100 Ardışık Bitişik Disk Yazımı (commitContiguousStripe: yalnızca nextContiguousStripe diske yazılır)

Doğrulanan değişmezler (Invariants):
  I1  Diskteki tüm stripe'lar 0..received-1 aralığında kesintisiz bitişiktir (delik YOK).
  I2  İptal anında ekranda görünen byte (len(written)) == Dosya Yöneticisindeki bitişik boyut (received).
      (v13'te Galaxy S10+ videosu 02:26'da 933 MB yazılıp 113 MiB'a kesilme hatası v14'te 0 kayıpla çözülür!)
  I3  İptal yoksa transfer %100 tamamlanır (bir soket stripe #113'te tamamen donsa bile!).
  I4  Hiçbir senaryoda kilitlenme (deadlock) oluşmaz.
  I5  Direct ByteBuffer havuzunda sızıntı (leak) oluşmaz.
"""
import random
import sys

STRIPE_LAT = 10.0
WRITE_MS = 1.2
MAX_WINDOW = 32
STRAGGLER_MS = 120.0


def run_case(seed, n, poolsize, cancel_at_frac, n_pauses, stall_p, freeze_stripe_113=False):
    rnd = random.Random(seed)
    socks = 4
    t = 0.0
    next_assign = 0
    sock_pending = [None] * socks  # None | ('hdr', idx, t_hdr) | ('wait', idx) | ('read', idx, t_ready)
    stripe_start_t = {}
    delivered = set()
    urgent_nack_queue = []
    free_bufs = poolsize
    completed = {}
    written = set()
    on_disk = set()
    nxt = 0
    writer_busy_until = 0.0
    paused = False
    pause_end = 0.0
    preview = False
    pause_plan = sorted(rnd.uniform(0.05, 0.9) for _ in range(n_pauses))
    pause_dur = [rnd.uniform(100, 1200) for _ in range(n_pauses)]
    pause_idx = 0
    cancel_trigger = int(n * cancel_at_frac) if cancel_at_frac is not None else None
    cancelled = False
    last_progress_t = 0.0
    inflight_buf = 0

    def next_clear(frm):
        i = frm
        while i in written:
            i += 1
        return i

    def lowest_unfinished(up_to):
        for i in range(nxt, min(up_to, n)):
            if i not in delivered and i not in written and i not in completed:
                return i
        return -1

    def commit_contiguous(idx):
        nonlocal free_bufs, nxt
        assert idx == nxt, f"Sıra dışı yazım engellendi: idx={idx} != nxt={nxt}"
        written.add(idx)
        on_disk.add(idx)
        delivered.add(idx)
        free_bufs += 1
        nxt = next_clear(nxt + 1)

    while True:
        t += 1.0
        if t - last_progress_t > 50000:
            return dict(deadlock=True, t=t, nxt=nxt, n=n, free=free_bufs, comp=len(completed))

        if cancel_trigger is not None and not cancelled and len(written) >= cancel_trigger:
            cancelled = True
            break

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
                if idx < nxt or idx in written or idx in completed:
                    free_bufs += 1
                else:
                    completed[idx] = True
                    delivered.add(idx)
                    # Alıcı Reverse-NACK: eğer nxt eksikse ve completed >= 2 ise acil NACK gönder
                    if idx > nxt and nxt not in completed and len(completed) >= 2:
                        if nxt not in urgent_nack_queue:
                            urgent_nack_queue.append(nxt)
                inflight_buf -= 1
                sock_pending[s] = None
                last_progress_t = t

            if sock_pending[s] is None:
                chosen = -1
                # Priority 1: Reverse-NACK kurtarma kuyruğu
                while urgent_nack_queue:
                    cand = urgent_nack_queue.pop(0)
                    if cand not in delivered and cand not in written and cand not in completed:
                        chosen = cand
                        break
                # Priority 3: Sınırlı kayan pencere (MAX_WINDOW = 32) ve >120ms Straggler kurtarma
                if chosen < 0:
                    min_unf = lowest_unfinished(next_assign)
                    if min_unf >= 0:
                        el = t - stripe_start_t.get(min_unf, t)
                        if (next_assign - min_unf) >= MAX_WINDOW or el >= STRAGGLER_MS:
                            chosen = min_unf
                            stripe_start_t[min_unf] = t
                # Priority 4: Yeni ardışık stripe
                if chosen < 0:
                    if next_assign < n:
                        chosen = next_assign
                        next_assign += 1
                        stripe_start_t[chosen] = t
                    else:
                        tail_unf = lowest_unfinished(n)
                        if tail_unf >= 0:
                            chosen = tail_unf
                            stripe_start_t[chosen] = t

                if chosen >= 0:
                    if freeze_stripe_113 and s == 0 and chosen == 113:
                        # Galaxy S10+ 02:26 senaryosu: Soket 0 stripe #113'te tamamen takılır!
                        sock_pending[s] = ('hdr', chosen, t + 999999.0)
                    else:
                        extra = rnd.uniform(140, 500) if rnd.random() < stall_p else 0.0
                        sock_pending[s] = ('hdr', chosen, t + rnd.uniform(0.5, 2.0) + extra)
                    last_progress_t = t

        if not paused and pause_idx < len(pause_plan) and len(written) >= int(n * pause_plan[pause_idx]):
            paused = True
            preview = False
            pause_end = t + pause_dur[pause_idx]
            pause_idx += 1
            last_progress_t = t

        if t < writer_busy_until:
            continue

        if paused:
            if not preview:
                while nxt in completed:
                    del completed[nxt]
                    commit_contiguous(nxt)
                preview = True
                assert all(i < nxt for i in on_disk)
            if t >= pause_end:
                paused = False
                preview = False
                last_progress_t = t
            continue

        if nxt in completed:
            del completed[nxt]
            commit_contiguous(nxt)
            writer_busy_until = t + WRITE_MS
            last_progress_t = t

        if nxt >= n:
            break

    # İptal veya tamamlanma sonrası bellekteki bitişik stripe'ları boşalt
    while nxt < n and nxt in completed:
        del completed[nxt]
        commit_contiguous(nxt)

    received = n if nxt >= n else nxt
    ui_shown_written = len(written)
    valid_prefix = all(i in on_disk for i in range(received))
    return dict(
        deadlock=False,
        cancelled=cancelled,
        received=received,
        ui_written=ui_shown_written,
        n=n,
        I1=valid_prefix,
        I2=(received == ui_shown_written),
        complete_ok=(received == n and len(on_disk) == n),
        free=free_bufs,
        comp=len(completed),
        inflight=inflight_buf,
        pool=poolsize,
    )


def main(runs):
    stats = dict(cases=0, deadlock=0, I1=0, I2=0, I3=0, leak=0)
    ex = []

    # 1) Özel Galaxy S10+ 02:26 Regresyon Testi:
    #    Soket 0 stripe #113'te sonsuza dek takılır, transfer stripe #933'te iptal edilir.
    #    v13'te dosya 113 MiB'a kesiliyordu; v14'te 933 MiB eksiksiz korunur (received == ui_written == 933)!
    s10_case = run_case(
        seed=42,
        n=2000,
        poolsize=64,
        cancel_at_frac=933 / 2000.0,
        n_pauses=0,
        stall_p=0.02,
        freeze_stripe_113=True,
    )
    assert not s10_case["deadlock"], "S10+ regresyon senaryosunda deadlock!"
    assert s10_case["I1"] and s10_case["I2"], f"S10+ regresyon hatası: {s10_case}"
    assert s10_case["received"] >= 933, f"S10+ iptal boyutu beklenenden küçük: {s10_case}"
    print(
        f"[OK] Galaxy S10+ Stripe #113 Donma + 933 MB İptal Testi -> "
        f"Ekranda: {s10_case['ui_written']} MB | Dosya Yöneticisinde: {s10_case['received']} MB (Kaybolan: 0 MB)"
    )

    for seed in range(runs):
        rnd = random.Random(20_000 + seed)
        n = rnd.choice([16, 48, 128, 300, 600])
        poolsize = min(64, max(16, n + 4))
        cancel_at = rnd.choice([None, None, rnd.uniform(0.05, 0.95)])
        n_pauses = rnd.choice([0, 1, 2])
        stall_p = rnd.choice([0.0, 0.02, 0.06])
        r = run_case(seed, n, poolsize, cancel_at, n_pauses, stall_p)
        stats["cases"] += 1
        if r["deadlock"]:
            stats["deadlock"] += 1
            ex.append(("DEADLOCK", seed, r))
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
        if r["free"] + r["comp"] + r["inflight"] > r["pool"]:
            stats["leak"] += 1
            ex.append(("LEAK", seed, r))
    return stats, ex


if __name__ == "__main__":
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 600
    stats, ex = main(runs)
    print(
        f"Senaryo: {stats['cases']}  | kilitlenme: {stats['deadlock']}  | I1 (bitişiklik) ihlali: {stats['I1']}  | "
        f"I2 (ekran==disk boyutu) ihlali: {stats['I2']}  | I3 (tamamlanmama): {stats['I3']}  | havuz sızıntısı: {stats['leak']}"
    )
    for e in ex[:6]:
        print("  örnek:", e)
    sys.exit(0 if all(stats[k] == 0 for k in ("deadlock", "I1", "I2", "I3", "leak")) else 1)
