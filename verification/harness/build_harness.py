#!/usr/bin/env python3
"""
Motorun GERÇEK kod dilimlerini (yeni motor + v12 baz çizgisi) dosyalardan keser ve Harness.kt üretir.
Kullanım:  python3 build_harness.py   (verification/harness dizininden)
"""
import os

HERE = os.path.dirname(os.path.abspath(__file__))
ENG_NEW = os.path.join(HERE, "..", "..", "app", "src", "main", "java", "com", "fasttransfer", "p2p", "transfer", "ZeroCopyTransferEngine.kt")
ENG_V12 = os.path.join(HERE, "..", "baseline", "ZeroCopyTransferEngine_v12.kt.txt")


def slice_between(src, start_marker, end_marker, from_idx=0):
    a = src.index(start_marker, from_idx)
    b = src.index(end_marker, a)
    return src[a:b]


new = open(ENG_NEW, encoding="utf-8").read()
v12 = open(ENG_V12, encoding="utf-8").read()

# --- yeni motor dilimleri ---
rx_start = "                        val totalStripes = ((expectedFileSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()\n                        // Havuz dosyanın"
rx_end = "                    } else {\n                        val streamChan = workerChannels.firstOrNull() ?: clientChannel"
RX_NEW = slice_between(new, rx_start, rx_end)

tx_start = "                        val nextStripeIdx = AtomicInteger(0)\n                        val activeChannels = workerChannels.take(confirmedWorkerCount)\n                        diagWorkers"
tx_end = "                    } else {\n                        val streamChan = workerChannels.firstOrNull() ?: controlChannel"
TX_NEW = slice_between(new, tx_start, tx_end)

# --- v12 baz çizgisi (karşılaştırma için) ---
tx12_start = "                        val nextStripeIdx = AtomicInteger(0)\n                        val activeChannels = workerChannels.take(confirmedWorkerCount)\n"
TX_V12 = slice_between(v12, tx12_start, tx_end)
rx12_start = "                        val totalStripes = ((expectedFileSize + STRIPE_BYTES - 1L) / STRIPE_BYTES.toLong()).toInt()\n                        val freeBuffers = ArrayBlockingQueue<ByteBuffer>(REORDER_RING_BUFFERS)"
RX_V12 = slice_between(v12, rx12_start, rx_end)

template = open(os.path.join(HERE, "Harness.kt.tpl"), encoding="utf-8").read()
out = (template
       .replace("/*__RX_NEW__*/", RX_NEW)
       .replace("/*__TX_NEW__*/", TX_NEW)
       .replace("/*__TX_V12__*/", TX_V12)
       .replace("/*__RX_V12__*/", RX_V12))
open(os.path.join(HERE, "Harness.kt"), "w", encoding="utf-8").write(out)
print(f"Harness.kt üretildi (RX_NEW={len(RX_NEW.splitlines())} satır, TX_NEW={len(TX_NEW.splitlines())}, "
      f"TX_V12={len(TX_V12.splitlines())}, RX_V12={len(RX_V12.splitlines())})")
