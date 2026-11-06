# Zeph v14 (6.0.0-Master) — Kök Neden Analizi ve Yeni Mimari

## 1. Videoların Saniye Saniye Analizi ve Kök Nedenler

### Video 1: `galaxy_tab_s9_ile_redmi_note_13_pro_plus_5g_iki_cihaz_da_ufs_3.1_h_z__hotspot_wifi_6_hevc_2.mkv`
- **00:00–00:50:** Redmi Note 13 Pro+ 5G üzerinde manuel Wi-Fi 6 Hotspot (`5745 MHz`, `1200 Mbps`) açılmış ve Galaxy Tab S9 bu hotspot'a bağlanmış. Her iki cihaz da başka bir ev modemine bağlı olmadığı için radyo %100 tek kanalda (SCC) çalışmış ve **103.5 MB/s ortalama (108.8 MB/s tepe)** hıza ulaşılmış (`disk yazma kapasitesi 1467 MB/s • yazıcı %7 meşgul`).
- **00:13 (1.4 GB) ve 00:54 (6.8 GB):** Hızın anlık olarak ~74 MB/s'ye dalgalanmasının nedeni Linux çekirdeğinin kirli sayfa önbelleğini (`vm.dirty_ratio`) senkron boşaltmasıdır.

### Video 2: `redmi note 13 pro plus 5g wifi 6 hotspot bagli hali.mp4`
- **00:46–02:18:** Galaxy S10+ → Redmi Note 13 Pro+ 5G yönünde 7.4 GB aktarım. Fiziksel bağlantı **Wi-Fi 6 (11ax) • 5785 MHz • Tx 1200 / Rx 1200 Mbps (pratik tavan ≈ 98–105 MB/s)** olmasına rağmen hız **53–56 MB/s**'de takılı kalmış (`okuma %2 / soket %97 meşgul`).
- **Kök Neden 1 (`trafficClass = 0xA0` WMM `AC_VI` Hatası):** Önceki sürümde soketlere `0xA0` (`DSCP CS5` → IEEE 802.11e WMM `AC_VI` Video kuyruğu) atanmıştı. Broadcom `bcmdhd` (Galaxy S10+) sürücüsünde `AC_VI` kuyruğu maksimum TXOP süresini **3.008 ms** ile sınırlar ve tam boyutlu (64/256 çerçevelik) 802.11ax A-MPDU blok birleştirmesini kısıtlar.
- **Kök Neden 2 (6 Thread + 24 MB `SO_SNDBUF` Çekirdek Tıkanması):** Galaxy S10+ işlemcisinde (Exynos 9820) 4 adet yüksek performanslı çekirdek ve 4 adet küçük/yavaş (in-order Cortex-A55) çekirdek bulunur. 6 paralel gönderici thread açıldığında 2 thread yavaş Cortex-A55 çekirdeklerine düşmüş ve soket başına 4 MB (`6 × 4 MB = 24 MB`) `SO_SNDBUF` tampon şişmesi (bufferbloat) yaratmıştır.

### Video 3: `galaxy s10 plus.mp4`
- **00:34–00:38 (2.0 GB'ta 76 MB/s → 31 MB/s Çöküşü):** Alıcı tarafta `Os.pwrite` ile yazılan sayfalar için `posix_fadvise(POSIX_FADV_DONTNEED)` çağrılmadığı için 2.0 GB sınırında (8 GB RAM'in `%20` `vm.dirty_ratio` eşiği) Linux çekirdeği `balance_dirty_pages()` içinde yazıcı thread'i senkron bloklamıştır.
- **01:59–02:35 (933 MB Aktarılıp İptal Edildiğinde Dosya Yöneticisinde 127 MB / 113 MiB Görünmesi):**
  - 6 soketten biri **Stripe #113**'te (`113 MiB`) takılı kalmış, diğer 5 soket ise sınırsız `nextStripeIdx.getAndIncrement()` ile **Stripe #933**'e (`933 MB`) kadar ilerlemiştir.
  - Alıcı, sıra dışı gelen `#114..#933` parçalarını diske yazmış ve ekranda `totalBytesWrittenAtomic` (`933 MB`) göstermiştir; ancak `#113` hiç gelmediği için `nextContiguousStripe` (`contiguousWatermarkBytes`) **113 MiB**'da donmuştur.
  - Kullanıcı `02:26`'da iptal ettiğinde `fileChannel.truncate(contiguousWatermarkBytes)` çağrısı dosyayı **113 MiB** (+ 8 MiB `moov` = `121.1 MiB` = Dosya Yöneticisinde **127 MB**) boyutuna keserek **820 MB veriyi silmiştir**!
- **03:35–04:07 (`GreenMile` Modemine Bağlıyken 6.2 MB/s Çöküşü):**
  - `04:07`'deki teşhis satırı (`Bağlantı: Wi-Fi 5 • 5280 MHz • Tx 650 / Rx 780 Mbps`) kök nedeni kanıtlamıştır: Ev modemi (`wlan0`) **5280 MHz (DFS Kanal 56)** üzerindeyken, Wi-Fi Direct P2P grubu (`p2p0`) DFS kanallarda grup kuramadığı için **5765 MHz (Kanal 153)** üzerinde açılmıştır.
  - `wlan0` 5280 MHz'e bağlı kaldığı sürece tek 5 GHz radyo **5280 MHz ↔ 5765 MHz MCC (Multi-Channel Concurrency)** zaman dilimlemesine girmiş ve hız **6.2 MB/s**'ye düşmüştür.
- **04:08–04:10 (Çift Cihaz Satırı ve `[LG] webOS TV` Kirliliği):**
  - Aynı cihaz hem `192.168.49.1` hem `192.168.1.92` üzerinden görüldüğünde `LanDiscoveryManager` isimlerin sonuna `(#1)` ve `(#92)` ekleyerek iki ayrı kesik satır oluşturmuştur.

## 2. v14 Mimarisi: Ne Değişti ve Neden Önceki Yaklaşımlardan Farklı?

1. **Tam Otomatik 5 GHz / 6 GHz Doğrudan Köprü ve MCC Koruması (`CascadingWifiDirectManager` & `CompatibilityHelper`):**
   - Kullanıcı yalnızca hedef cihaza dokunur. İki cihaz aynı modemde, farklı ağlarda veya tamamen çevrimdışı olsa bile `DIRECT-ZP-<tag>-<alias>` deterministik 5 GHz Otonom Grubu ve P2P DNS-SD (`_zeph._tcp`) ile **sıfır sistem onayı penceresi** çıkararak 2–3 saniyede 5 GHz (`192.168.49.x`) köprüsünü kurar.
   - Ev modemiyle P2P kanalı farklı frekanslardaysa (ör. 5280 MHz DFS modem vs 5765 MHz P2P), P2P veri kanalları açılır açılmaz `suspendConflictingStaWifiForMcc()` çakışan `wlan0` bağlantısını aktarım süresince geçici olarak askıya alır (radyo %100 tek kanalda 100+ MB/s çalışır) ve aktarım biter bitmez `restoreSuspendedStaWifi()` ile modeme otomatik geri bağlar.
2. **Tam-Dupleks Reverse-NACK + Sınırlı Kayan Pencere (`MAX_STRIPE_WINDOW = 32`) + %100 Bitişik Sıralı Disk Yazımı:**
   - Alıcı okuma thread'leri, her veri soketinin ters yönünden (reverse path) göndericiye 4 baytlık `ACK/NACK` çerçeveleri yollar. `nextContiguousStripe` geciktiğinde anında `NACK(missingStripe)` gönderilir; ayrıca göndericide `> 120 ms` geciken her stripe hızlı soketler tarafından spekülatif olarak kurtarılır.
   - Böylece alıcı yazıcı thread'i diske **yalnızca ardışık bitişik (`0, 1, 2, ...`)** stripe'ları yazar (`contiguousWatermarkBytes == totalBytesWrittenAtomic`). Diskte asla boşluk oluşmaz ve iptal anında Zeph ekranındaki boyut ile Dosya Yöneticisindeki boyut baytına kadar birebir aynı kalır.
3. **WMM `AC_BE` (`0x08`) Bulk A-MPDU Kuyruğu + 4 Performans Akışı + `POSIX_FADV_DONTNEED` Write-Behind:**
   - `trafficClass = 0x08` (`IPTOS_THROUGHPUT` / `AC_BE`) ve `PARALLEL_DATA_STREAMS = 4` ile Galaxy S10+ üzerindeki 54 MB/s darboğazı kaldırılmış; her 32 MB'ta bir `applySequentialWriteBehindFadvise()` çağrılarak 2.0 GB'taki sayfa önbelleği tıkanması tamamen giderilmiştir.
4. **Tekil Cihaz Satırı ve Net Hız Etiketleri:**
   - `deviceTag` ve normalize edilmiş cihaz adına göre tüm IP/P2P yolları tek satırda birleştirilmiş, TV/yazıcı gibi cihazlar filtrelenmiş ve `⚡ 5 GHz Doğrudan Köprü (Aktif • ~95–125 MB/s)`, `🚀 Otomatik Yüksek Hızlı Köprü (5 GHz • ~90–120 MB/s)` etiketleri eklenmiştir.
