# Doğrulama düzeneği (APK'ya girmez)

Bu klasör Gradle derlemesinin **dışındadır**; yalnızca `docs/ANALIZ_VE_DEGISIKLIKLER.md` içindeki iddiaların nasıl
doğrulandığını gösterir ve yeniden çalıştırılabilir kılar.

Gereksinim: JDK 17+, Kotlin 1.9.x derleyicisi (`kotlinc`), Python 3. `/tmp` altında ~1,5 GB boş alan.

## 1) Model simülasyonları (Python)
```
python3 sim_pause_resume.py 150   # v12 muhasebesi vs düzeltilmiş: duraklat→devam sonrası tamamlanmış transfer bozuluyor mu?
python3 sim_impl.py 1200          # uygulanan algoritma: rastgele duraklat/devam/iptal/yinelenen stripe; invariant + kilitlenme testi
```

## 2) Gerçek motor kodunu JVM'de çalıştıran düzenek (Kotlin)
`build_harness.py`, `ZeroCopyTransferEngine.kt` içindeki **gerçek** gönderici/alıcı kod bloklarını (ve
`baseline/ZeroCopyTransferEngine_v12.kt.txt` içindeki v12 bloklarını) dosyadan keser; yalnızca Android sınıfları için ince
bir uyumluluk katmanı (`Process`, `Mp4FastStartHelper`, `TransferProgress`…) ekler ve loopback TCP üzerinde çalıştırır.
```
cd harness
python3 build_harness.py
kotlinc Harness.kt -include-runtime -d harness.jar
java -Xmx2g -jar harness.jar correct   # temiz/sınır boyutlar/okuma hatası/soket kaybı/duraklat-devam/hepsi birden/jitter+yinelenen/iptal
java -Xmx2g -jar harness.jar cancel    # Wi-Fi benzeri yavaş akışta rastgele anlarda iptal: korunan önek bayt bayt doğru mu?
java -Xmx2g -jar harness.jar v12bug    # v12 alıcı kodu vs yeni alıcı: aynı jitter + duraklat/devam (v12 hatasının gerçek kodda tekrarı)
java -Xmx2g -jar harness.jar bench     # loopback mikro-kıyas (Wi-Fi hızını TEMSİL ETMEZ)
```
Başarılı koşu `TÜM HARNESS TESTLERİ GEÇTİ` yazar ve çıkış kodu 0 olur.

## Sınırlar
Android'e özgü yollar (FUSE, `Os.pread`, gerçek Wi-Fi, WifiP2pManager) JVM'de çalışmaz. Bu düzenek algoritmanın
**doğruluğunu ve eşzamanlılık güvenliğini** doğrular; cihazdaki hızı ölçmez.
