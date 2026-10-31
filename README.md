# FastTransfer P2P (Kernel Zero-Copy & Cascading 6GHz/5GHz/2.4GHz Android Projesi)

Bu proje, ekteki videodaki **LocalSend** tabanlı koyu turkuaz/arduvaz (`#101818` / `#6CD4FF`) kullanıcı arayüzünü birebir uygulayan, **Android 5.0 (API 21)** ile **Android 15 (API 35)** arasındaki tüm cihazlarda çalışan ve donanımın desteklediği en yüksek ortak fiziksel kablosuz hızda (**Kernel Zero-Copy**) veri aktarımı yapan uçtan uca eksiksiz Android projesidir.

## Öne Çıkan Teknik Özellikler

1. **Kademeli Frekans Müzakeresi (6 GHz -> 5 GHz -> 2.4 GHz Fallback):**
   - `CascadingWifiDirectManager` önce yüksek hızlı 5 GHz / 6 GHz (`WifiP2pConfig.Builder().setGroupOperatingBand(...)`) kanalını dener.
   - Karşı cihaz eski bir modelse ve yalnızca 2.4 GHz destekliyorsa (`onFailure`), uygulama hata verip durmaz; milisaniyeler içinde standart 2.4 GHz `WifiP2pConfig` moduna düşerek bağlantıyı kurar.
   - Aynı yerel ağ (LAN) veya 5 GHz / 2.4 GHz Mobil Erişim Noktası (Hotspot) üzerindeyken `LanDiscoveryManager` (`NsdManager` mDNS + UDP Broadcast) ile doğrudan IP üzerinden anında aktarım başlatır.

2. **Paylaş Menüsü Sıfır Bekleme (Zero-Cache Share Sheet):**
   - `AndroidManifest.xml` içinde `ACTION_SEND` ve `ACTION_SEND_MULTIPLE` (`*/*`) tanımlıdır.
   - Galeri veya Dosya Yöneticisinden **"Paylaş"** denildiğinde URI'ler **kesinlikle `cacheDir` veya `filesDir` içine kopyalanmaz**. `contentResolver.openFileDescriptor(uri, "r")` ile doğrudan orijinal disk konumundan ağ soketine akıtılır.

3. **%5 Kısmi Dosya Koruma Kuralı ve Zero-Copy Motoru (`ZeroCopyTransferEngine`):**
   - Dosyalar `part1`, `part2` şeklinde bölünmez ve birleştirme (merge) yapılmaz.
   - Alıcı cihazda dosyalar ilk bayttan itibaren doğrudan `Download` klasörüne (`Environment.DIRECTORY_DOWNLOADS`) gerçek adı ve uzantısıyla (`.mp4`, `.mkv` vb.) yazılır.
   - Transfer **%5'te (veya herhangi bir anda) durdurulduğunda ya da bağlantı koptuğunda**:
     1. `file.delete()` **ASLA çağrılmaz**.
     2. `fileChannel.truncate(totalBytesReceived)` ile dosya boyutu o ana kadar inen gerçek bayt miktarına kesilir.
     3. `fileChannel.force(true)` (`fsync`) ile tampon diske mühürlenir ve `MediaScannerConnection.scanFile()` tetiklenir.
     4. Kullanıcı arayüzdeki **"Kısmi Dosyayı Test Et / Aç"** butonuna basarak %5'lik videoyu anında izleyebilir.

4. **GitHub Actions ile Tek Tıkla Otomatik APK Derleme:**
   - `.github/workflows/android-build.yml` dosyası `gradle/actions/setup-gradle@v3` ve **JDK 17 (Temurin)** kullanarak `gradle assembleDebug` komutunu çalıştırır ve derlenen APK'yı `FastTransfer-APK` adıyla Artifacts bölümüne yükler.
