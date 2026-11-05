# Zeph v12.1 — Analiz, Değişiklikler ve Kanıt Düzeyleri

Bu belge dört soruyu yanıtlar: **(1)** v12 neden en iyi sonucu veriyor, **(2)** v13+ müdahaleleri neden hızı düşürdü,
**(3)** v12.1'de tam olarak ne değişti ve her değişiklik için *ne kanıtlandı / ne yalnızca beklenti*, **(4)** hedefler
donanım açısından nerede duruyor.

> **Birim notu.** Uygulamanın "MB/s" değeri aslında **MiB/s**'dir (`computeSustainedSpeedMbps` 1024×1024'e böler).
> Bu belgede uygulama birimi (MiB/s) kullanılır; Mbps = kablosuz bağlantı hızı (bit/s).

## 0. Dürüstlük notu (önce bunu okuyun)

* Bu çalışma **gerçek cihaz olmadan** yapıldı. Bu belgede hiçbir yerde "şu cihazda şu kadar hızlandı" yazmıyor; yazamam.
* Her değişiklik bir **kanıt düzeyi** taşır: **[K] kanıtlandı** (gerçek motor kodu çalıştırılarak / birim testle),
  **[D] doğrulandı** (resmî dokümana veya kod sabitlerinden hesaba dayalı), **[T] teorik** (mekanizma bilinir, bu ortamda ölçülemedi).
* Bu sürümün asıl kazancı **hız değil, doğruluk ve bağlantı süresi**dir. Hız tarafında v12 zaten radyo tavanına yakındır (bkz. §1);
  yazılımla yapılabilecek tek şey tavanın *altında* kalmaya yol açan yazılım kaynaklı duraksamaları kaldırmak ve
  darboğazı **ölçülebilir** kılmaktır (Analiz satırları).

## 1. Donanım tavanı: hedefler yazılımla garanti edilemez

Gerçek TCP verimi ≈ PHY hızı × 0,65–0,70 (Cisco 802.11ac throughput rehberindeki pratik kural).

| Bağlantı (80 MHz, 2x2) | PHY hızı | Pratik tavan (Mbps) | Pratik tavan (MiB/s) |
|---|---|---|---|
| 802.11ac (Wi-Fi 5), VHT80 2SS | 866,7 Mbps | 563–607 | **67–72** |
| 802.11ax (Wi-Fi 6), HE80 2SS | 1201 Mbps | 781–841 | **93–100** |
| 802.11ax, 160 MHz 2SS | 2402 Mbps | 1561–1681 | 186–200 |

Sizin v12 ölçümleriniz bu tavanlara oturuyor:

* **Galaxy S10+: 67–78 MiB/s** (562–654 Mbps = %65–75 verim) → 11ac/VHT80 2x2 tavanı (67–72) ile örtüşüyor.
* **Redmi Note 13 Pro+: 86–96 MiB/s** (721–805 Mbps = %60–67 verim) → 11ax/HE80 2x2 bandında (93–100'e yakın).

Sonuç: **v12 zaten fiziksel tavanın %85–100'ünde çalışıyor.** "S10+ üzerinde 90+ MiB/s" = 755 Mbps = VHT80 bağlantı için
%87 verim demektir; bu, 11ac 2x2 80 MHz bağlantıda pratikte ulaşılamaz. Ancak bağlantı **HE80 2x2 (Wi-Fi 6)**
olarak kurulursa mümkündür. "110–140+ MiB/s" = 923–1174 Mbps için ≥1,4–1,8 Gbps PHY (160 MHz Wi-Fi 6/6E/7) gerekir;
UFS 3.1 bunun darboğazı değildir (sıralı hızı Gbps seviyesinin çok üstünde), **radyo** darboğazdır.

**Pratik kaldıraç (cihazda deneyin):** Redmi'nin hotspot'unu açıp S10+'ı istemci yapın. S10+ Wi-Fi 6 destekler
(Broadcom BCM4375, 2x2, HE). Transfer sonunda gösterilen **"Bağlantı: Wi-Fi 6 (11ax) • … • Tx 1201 Mbps"** satırı tavanın
≈93–100 MiB/s olduğunu doğrular; "Wi-Fi 5 (11ac) 867 Mbps" görürseniz tavan 67–72 MiB/s'dir ve yazılım bunu aşamaz.
(Bu satır yalnızca Wi-Fi *istemci* tarafında ve LAN/hotspot yolunda görünür; P2P grubunda Android bu bilgiyi vermez.)

## 2. v12 neden en iyi? (kod okumasına dayalı)

1. **Bariyersiz iş-çalma gönderici.** 6 worker, her biri kendi dosya tanıtıcısıyla, `nextStripeIdx.getAndIncrement()` ile
   1 MiB'lık sayfa-hizalı stripe alır. Hiçbir worker bir diğerini beklemez; yavaşlayan soket kendiliğinden daha az stripe alır.
2. **Paralel okuma.** 6 bağımsız `pread` aynı anda uçuştadır: FUSE arka plan thread'lerine eşzamanlı istek gider ve depolama
   gecikmesi ağ yazmasıyla örtüşür (tek thread'li okuma bunu kaybeder).
3. **Alıcıda tek yazıcı + 6 okuyucu + 64 MiB havuz.** Okuyucular çekirdek RX tamponlarını sürekli boşaltır (TCP penceresi
   açık kalır); tek yazıcı hizalı `pwrite` yapar (FUSE'ta yazma çekişmesi yok).
4. **Anti-HoL kuralı** (kuyrukta ≥4 stripe varken en düşüğü sırasız yaz): geç kalan *tek* stripe yüzünden havuz tükenmez,
   soketler asla durmaz. Bu kuralı kaldıran bir mutantı simülasyonda **kilitlenme** ile yakaladım (bkz. §6) — yani v12'nin en
   kritik tasarım kararı budur.
5. **Hizalı I/O:** ofsetler 1 MiB katı, tampon konumu 0.
6. Doğru arayüze soket bağlama, TCP_NODELAY, yüksek öncelikli thread'ler, Wi-Fi/Wake kilitleri, transfer sırasında keşif
   beacon'larının durdurulması (radyo kanaldan ayrılmasın).

## 3. v13+ müdahaleleri neden hızı düşürdü?

> v13+ kaynak kodu elimde yok. Aşağıdakiler, sizin listenizdeki her madde için v12 kodu ve Linux/Android davranışından
> türetilmiş **mekanizmalardır**; 6–25 MiB/s rakamları sizin ölçümünüzdür.

| Müdahale | Neden zararlı (mekanizma) |
|---|---|
| **Tek `diskPrefetchThread`** | Tüm okumaları tek thread'e serileştirir: FUSE'ta aynı anda tek istek uçuşta kalır, hız = tek-thread FUSE okuma hızıdır; thread bir yavaş istekte bloklanırsa tüm boru hattı aç kalır; ayrıca ara kuyruk + ek bellek kopyası getirir. v12'de 6 eşzamanlı okuma bu gecikmeyi saklar. |
| **FUSE üzerinde `FileChannel.transferTo`** | `sendfile/splice` yolu sayfa önbelleği + FUSE readahead'ine (~128 KiB penceresi) bağlıdır; çekirdek yolu desteklemezse Java `transferTo`, 8 KiB'lık ara tampon döngüsüne düşer. İki durumda da v12'nin 1 MiB'lık hizalı `pread`'inden çok küçük istekler üretir. |
| **Aşırı büyük `SO_SNDBUF`** | Açık `SO_SNDBUF/RCVBUF` ayarı o soketin otomatik ayarını kilitler; çok büyük tampon bufferbloat (RTT şişer, RTO uzar), sistem genelinde `tcp_mem` baskısı ve iptalde çok büyük "uçuştaki" veri demektir. v12'nin 4/16 MiB'ı çalışan ayardır; **bu sürümde değiştirilmedi.** |
| **`MAX_SENDER_STRIPE_SPREAD` / `MAX_REORDER_WINDOW`** | Kafa-sırası engellemesini (HoL) geri getirir: Wi-Fi'de tek bir soketin 200–1000 ms'lik RTO duraksaması, pencere dolunca *tüm* soketleri durdurur. Verim ≈ pencere/duraksama süresi'ne çöker (ör. 16 MiB / 0,5 s ≈ 32 MiB/s) — 6–25 MiB/s bandının tipik açıklaması. v12'nin Anti-HoL'u tam bunu önler. |
| **450 ms "straggler rescue"** | Wi-Fi'de 100–400 ms RTT sıçramaları normaldir; eşik yanlış-pozitifler üretir ve aynı veriyi ikinci soketten tekrar yollayarak kıt hava süresini boşa harcar (goodput düşer); hot-path'e dedup/kayıt yükü ekler. |
| **`join` sırasında erken ikinci `connect()`** | Birinci P2P müzakere sürerken ikinci `WifiP2pManager.connect()` çağrısı supplicant/firmware durumunu sıfırlar (BUSY / grup yeniden kurulur); 1–2 sn'lik katılım tekrar eden zaman aşımlarına döner. v12 ikinci `connect()`'i yalnızca birincinin *başarısızlık geri çağrısından* çağırır (örtüşme yok). |
| **`position = 8` ile hizasız `Os.pread`** | Dosya ofseti 1 MiB hizalı kalırken bellek hedefi 8 bayt kayar: her dosya sayfası iki bellek sayfasına taşar, çekirdeğin sayfa-başı kopyaları bölünür, hızlı yollar kapanır. (Bu sürümde başlık+veri **tek gathering write** ile yollanır; veri tamponu konum 0'da kalır, yani v12 ile aynı hizalama.) |

## 4. Bulunan kritik hata: duraklat → devam, tamamlanmış transferi ~%5'e kısaltıyor  [K]

Şikâyetiniz ("2 GB aktarıyor ama ~100 MB korunuyor, özellikle hotspot'ta") için **kök neden v12 alıcısında**:

1. Anti-HoL nedeniyle diskte, watermark'ın (ilk eksik stripe) ötesinde **geçerli sıra-dışı stripe'lar** bulunur.
2. **Devam et** anında v12 `writtenStripesBitSet.clear(next, …)` ile bunları "yazılmamış" sayar,
   `totalBytesWrittenAtomic`'i watermark'a **sıfırlar** ve (MP4'te) `restoreOriginalMdatForResume` ile dosyayı watermark'a
   **truncate eder** — gönderici o stripe'ları bir daha yollamaz.
3. Watermark kalıcı olarak takılır. Bitişte `totalBytesWritten < expected` olduğundan alıcı dosyayı **watermark'a truncate eder**
   (duraklatma noktası, ör. %5 ≈ 100 MB) ve "Kısmi dosya korundu" der; gönderici ise her şeyi yolladığı için %100 görür.
4. Hotspot/Wi-Fi'de stripe gecikmeleri (TCP RTO) daha sık olduğundan "geç kalan stripe + duraklatma" çakışması daha sık yaşanır.

**Kanıt:**
* Model simülasyonu (`verification/sim_pause_resume.py`): v12 mantığı duraklat/devam döngülerinin ≈%35–41'inde bozuyor; düzeltilmiş mantık 0.
* **Gerçek v12 alıcı kodu** (dosyanızdan dilimlenip JVM'de, aynı jitter + duraklat/devam ile çalıştırıldı):
  **v12 alıcı 17/20 koşuda eksik/kısaltılmış dosya bıraktı; yeni alıcı 0/20.** (Jitter modeli agresiftir — stripe başına %2
  olasılıkla 150–700 ms duraksama; gerçek oran bağlantıya göre değişir, **mekanizma deterministiktir**.)

## 5. v12.1 değişiklikleri ve kanıt düzeyleri

Veri hattı (6 soket, iş-çalma, 6 okuyucu + tek yazıcı, Anti-HoL, 1 MiB stripe, 4/16 MiB soket tamponu) **değişmedi**.

| # | Değişiklik | Neden / mekanizma | Kanıt |
|---|---|---|---|
| 1 | Alıcı defteri (ledger): bitset hiçbir koşulda geri alınmaz, sayaç yalnızca benzersiz stripe'ları sayar, kısa yazma hata sayılır | §4 | **[K]** gerçek kod harness'ı + model |
| 2 | Duraklatma modu: yalnızca bitişik stripe yaz; önizleme (moov) yalnızca watermark ötesinde diskte stripe yokken uygulanır; devamda yalnızca önizleme geri alınır | moov kuyruğunun `truncate`'i geçerli veriyi yok edemez | **[K]** 1200 senaryo: 0 kilitlenme/0 ihlal; mutasyon testi mutantları yakaladı |
| 3 | Gönderici: **önce oku, sonra** başlık+veri; başarısız stripe yeniden kuyruğa; alıcı yinelenen stripe'ı atar | v12 başlığı okumadan önce yolluyordu → okuma hatasında alıcı hiç gelmeyecek veriyi bekler, kalıcı delik | **[K]** harness T2/T3/T6/T7 (okuma hatası, soket kaybı, yinelenen) |
| 4 | Başlık+veri **tek gathering write (`writev`)** | küçük 8 baytlık TCP segmenti + bir syscall/stripe azalır (teorik ≤~%1 hava süresi) | **[T]** loopback'te v12 574 vs yeni 566 MiB/s (tek çekirdek, gürültü içinde) → **ölçülebilir kazanç iddia edilmiyor**, ek yük de ölçülmedi |
| 5 | Alıcı, **diskte korunan bayt sayısını** göndericiye bildirir; gönderici bunu gösterir; iptalde veri soketleri RST, kontrol kanalı kısa süre açık | v12'de gönderici kendi soket sayacını (çekirdek tamponundaki ~100 MiB dahil) "gönderildi" diye gösteriyordu; eski alıcı hata durumunda hiç yanıt vermeyip göndericiyi 30 sn bekletiyordu | **[K]** alıcı sayısı/önek doğruluğu (T8, T8b); UI akışı cihazda denenmedi |
| 6 | İptal sonrası arayüz: "Kısmi dosya kaydediliyor…" → motor kesin boyutu bildirince "Korundu"; bayat güncellemeler yok sayılır; 0 bayt = "Aktarım İptal Edildi"; `stop()` sonlandırmayı ≤1,5 sn bekler | "Durdur + uygulamayı kapat" akışında dosya yarım kalmasın | **[T]** mantık; cihazda denenmedi |
| 7 | Aktarım ortasında kopma artık "Bağlantı Kurulamadı" değil, kısmi sonuç + neden olarak gösterilir; toplu gönderimde kısmi sonuçtan sonra durulur | v12 her istisnayı "ulaşılamadı" sayıyordu | **[T]** |
| 8 | `fsync` kritik yoldan çıkarıldı (alıcı bitişi + `Mp4FastStartHelper.applyPartialMoovTail`) | büyük dosyada "Tamamlandı" ve alıcı onayı fsync süresi kadar (sn) gecikiyordu; sayfa önbelleği uygulama kapansa da yazılır | **[T]** kazanç = fsync süresi (cihaza/dosyaya bağlı) |
| 9 | Havuz dosyaya göre boyutlanır (≤64 MiB) | 5 MiB'lık dosya için 64 MiB `allocateDirect` (sıfırlama + GC) her dosyada tekrarlanıyordu | **[T]** toplu transferde |
| 10 | Doğrudan bağlantı tespiti: **tüm** ağların geçidi + yerel tethering arayüzü (adından bağımsız); alt ağ eşleştirme önek uzunluğuna duyarlı | v12 yalnızca `activeNetwork`'e bakıyordu (mobil veri açıkken hotspot kaçıyor → gereksiz Wi-Fi Direct müzakeresi); `/24` string öneki `/23`,`/16` ağlarda yanlış | **[K]** alt ağ aritmetiği 19 birim test; cihazdaki arayüz davranışı **[T]** |
| 11 | Wi-Fi Direct yükseltmesi: yalnızca ≥384 MiB ve doğrudan olmayan bağlantıda; Faz B yalnızca Faz A **hiç denenmediyse** | §6 | **[D]** kod sabitlerinden bütçe hesabı |
| 12 | 6 veri soketi **eşzamanlı** bağlanır; LAN/hotspot'ta 3 deneme | ardışık açılış 6×RTT (+ hatada 6×8 sn'ye kadar) | **[T]** |
| 13 | İkinci Wi-Fi kilidi (`HIGH_PERF`) + CPU kilidi 20/30 dk → 4 sa | `LOW_LATENCY` yalnızca ekran açık + ön plandayken aktif (AOSP javadoc); ekran kilitlenince Wi-Fi güç tasarrufuna girebilir | **[D]** AOSP dokümanı |
| 14 | Transfer sonunda **Analiz** (disk yazma kapasitesi, yazıcı/okuyucu meşguliyeti, sınır: DİSK/AĞ) ve **Wi-Fi bağlantı** satırı | ölçmeden optimizasyon v13+ hatasını tekrarlar; bu satırlar darboğazı doğrudan gösterir | **[T]** gözlem; veri yoluna dokunmaz |
| 15 | Alıcı: 0 bayt korunursa boş dosya silinir; "depolama doldu" nedeni gösterilir | | **[T]** |

## 6. Bağlantı süresi bütçesi (kod sabitlerinden)  **[D]**

Onaydan sonra v12'de: grup bekleme ≤3 sn + Faz A (≤~5 sn) + Faz B (≤5,5 sn + katılım ≤3,4 sn + IP ≤1,65 sn) →
yükseltme başarısızsa **≈13–18 sn**. v12.1'de:

| Durum | v12 | v12.1 |
|---|---|---|
| Hotspot / P2P (doğrudan) | tespit kaçarsa ≈13–18 sn | ≈0 (tespit güçlendirildi) |
| LAN, <384 MiB | 3–18 sn (her LAN transferinde müzakere) | ≈0 |
| LAN, ≥384 MiB, yükseltme çalışırsa | 2–7 sn | aynı |
| LAN, ≥384 MiB, yükseltme başarısızsa | ≈13–18 sn | en kötü ≈8 sn (tipik 3–5 sn), sonra LAN |

Not: Kaç GB'lık transferde P2P'nin LAN'a göre kâr ettiği (≈300 MiB başa baş) router'ınıza göre değişir; sabit
`UPGRADE_MIN_TOTAL_BYTES` (`ZeroCopyTransferEngine.kt`) ile ayarlanır.

## 7. Doğrulama özeti

* **Derleme:** MainActivity hariç tüm kaynaklar Kotlin 1.9.24 + gerçek `android.jar` (API 35) ile **0 hata** (orijinal v12 de 0 hata,
  harness doğrulandı). MainActivity AndroidX/ViewBinding olmadan derlenemez; sözdizimi geçerli, hata farkı yalnızca
  çözülemeyen `binding` kaynaklı kaskad (+3 "variable expected", −1 kaldırılan Toast).
* **Gerçek motor kodu, JVM, loopback, hata enjeksiyonlu** (`verification/harness`): temiz + sınır boyutlar (1 B… 200 MiB),
  okuma hataları, soket kaybı, iki taraflı duraklat/devam, hepsi birden, jitter + yinelenen stripe, rastgele anlarda iptal →
  **SHA-256 ile bayt bayt eşit / korunan önek bayt bayt doğru; 0 kilitlenme.**
* **Mutasyon testi:** v12 hatası, koşulsuz önizleme-truncate ve normal moddaki Anti-HoL'un kaldırılması harness/simülasyonda
  yakalandı (Anti-HoL kaldırılınca **kilitlenme**). Duraklatma modundaki "taşma eşiği" mutantı ayırt edilemedi:
  o eşik **savunmacı** bir önlemdir (uzun TCP RTO'da önizlemenin bloke olmaması için), kazancı kanıtlanmadı.
* **Sınırlar:** Android'e özgü yollar (FUSE, `Os.pread`, gerçek Wi-Fi) JVM'de çalışmaz; harness yalnızca algoritma ve
  eşzamanlılık mantığını doğrular. Sandbox tek çekirdekliydi (mikro-kıyas buna göre yorumlanmalı).

## 8. Cihazda ölçüm planı (bana ne göndermelisiniz)

1. Her iki cihaza v12.1'i kurun. 2–4 GB'lık bir dosyayı **(a)** Redmi hotspot'u + S10+ istemci, **(b)** aynı Wi-Fi/LAN ile gönderin.
2. Tamamlanınca iki cihazdaki **tüm mesajı** (Tamamlandı • Süre • Ort. Hız / **Analiz** / **Bağlantı** satırları) paylaşın.
   * "Sınır: AĞ/Wi-Fi" + "Bağlantı: … 867 Mbps" → tavan radyo; yazılımla aşılamaz.
   * "Sınır: DİSK YAZMA" ve disk yazma kapasitesi ≈ aktarım hızı → alıcıda FUSE/UFS darboğazı; o zaman (ve yalnızca o zaman)
     MediaStore tabanlı FUSE-atlayan yazma yolu ölçülerek eklenmeli.
3. Duraklat→devam→tamamla: iki cihazdaki dosya boyutu ve SHA-256 eşit olmalı. %5–10'da **iptal**: iki cihaz aynı kısmi boyutu
   göstermeli; kısmi video açılmalı.
4. Bağlantı süresi: "Gönder"→onay→ilk bayt süresini hotspot ve LAN'da not edin.

## 9. Bilinçli olarak YAPILMAYANLAR

* Soket tamponları, akış sayısı, stripe boyutu, Anti-HoL eşiği, iş-çalma, okuyucu/yazıcı thread yapısı: **dokunulmadı**.
* FUSE'u atlayan alıcı yazma yolu (MediaStore + `ContentResolver` fd): AOSP dokümanı bu yolun FUSE yükünden kaçındığını söylüyor
  ve S10+ gibi Android 12'ye yükseltilmiş cihazlar FUSE passthrough kullanamıyor — **olası** bir kazanç — ancak ölçmeden
  eklemek v13+ hatasını tekrarlamak olur. Önce Analiz satırı gerekli olup olmadığını göstermeli.
* Wi-Fi Direct zaman aşımlarının kendisi (join 3,4 sn, grup 3,5 sn): değiştirilmedi; yalnızca *kaç faz* denendiği/ne zaman denendiği.
