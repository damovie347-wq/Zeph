package com.fasttransfer.p2p

import android.app.Dialog
import android.content.Context
import android.content.Intent
import android.graphics.Color
import android.graphics.drawable.ColorDrawable
import android.net.Uri
import android.os.Bundle
import android.os.StrictMode
import android.text.Editable
import android.text.TextWatcher
import android.view.Gravity
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.view.Window
import android.widget.EditText
import android.widget.ImageButton
import android.widget.ImageView
import android.widget.PopupWindow
import android.widget.ProgressBar
import android.widget.TextView
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.fasttransfer.p2p.databinding.ActivityMainBinding
import com.fasttransfer.p2p.model.ApkAppItem
import com.fasttransfer.p2p.model.PeerDevice
import com.fasttransfer.p2p.model.SendMode
import com.fasttransfer.p2p.model.TransferItem
import com.fasttransfer.p2p.model.TransferProgress
import com.fasttransfer.p2p.model.TransferStatus
import com.fasttransfer.p2p.network.CascadingWifiDirectManager
import com.fasttransfer.p2p.network.LanDiscoveryManager
import com.fasttransfer.p2p.service.TransferForegroundService
import com.fasttransfer.p2p.transfer.ZeroCopyTransferEngine
import com.fasttransfer.p2p.ui.ApkListAdapter
import com.fasttransfer.p2p.ui.NearbyDeviceAdapter
import com.fasttransfer.p2p.util.CompatibilityHelper
import com.fasttransfer.p2p.util.StorageHelper
import com.google.android.material.bottomsheet.BottomSheetDialog
import com.google.android.material.switchmaterial.SwitchMaterial
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.text.DecimalFormat
import java.util.UUID

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding

    private lateinit var wifiDirectManager: CascadingWifiDirectManager
    private lateinit var lanDiscoveryManager: LanDiscoveryManager
    private lateinit var transferEngine: ZeroCopyTransferEngine
    private lateinit var nearbyAdapter: NearbyDeviceAdapter

    private val selectedTransferItems = mutableListOf<TransferItem>()
    private val p2pPeers = mutableListOf<PeerDevice>()
    private val lanPeers = mutableListOf<PeerDevice>()
    private val favoriteIps = mutableSetOf<String>()

    private var currentSendMode: SendMode = SendMode.SINGLE_RECIPIENT
    private var deviceAlias: String = "Kind Carrot"
    private var lastSavedOrPartialFilePath: String? = null
    private var pendingPeerForDirectTransfer: PeerDevice? = null

    private var skeletonAnimationJob: Job? = null

    // Activity Result Launchers (File, Media, Folder, Permissions)
    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) {
        wifiDirectManager.discoverPeers()
        lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
    }

    private val pickFilesLauncher = registerForActivityResult(
        ActivityResultContracts.OpenMultipleDocuments()
    ) { uris: List<Uri> ->
        if (uris.isNotEmpty()) {
            for (uri in uris) {
                try {
                    contentResolver.takePersistableUriPermission(
                        uri,
                        Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                } catch (_: Throwable) {
                }
                StorageHelper.buildTransferItemFromUri(this, uri)?.let { item ->
                    selectedTransferItems.add(item)
                }
            }
            refreshSelectionUi()
        }
    }

    private val pickMediaLauncher = registerForActivityResult(
        ActivityResultContracts.GetMultipleContents()
    ) { uris: List<Uri> ->
        if (uris.isNotEmpty()) {
            for (uri in uris) {
                StorageHelper.buildTransferItemFromUri(this, uri)?.let { item ->
                    selectedTransferItems.add(item)
                }
            }
            refreshSelectionUi()
        }
    }

    private val pickFolderLauncher = registerForActivityResult(
        ActivityResultContracts.OpenDocumentTree()
    ) { treeUri: Uri? ->
        if (treeUri != null) {
            try {
                contentResolver.takePersistableUriPermission(
                    treeUri,
                    Intent.FLAG_GRANT_READ_URI_PERMISSION
                )
            } catch (_: Throwable) {
            }
            lifecycleScope.launch(Dispatchers.IO) {
                val folderItems = StorageHelper.buildTransferItemsFromTreeUri(this@MainActivity, treeUri)
                withContext(Dispatchers.Main) {
                    if (folderItems.isEmpty()) {
                        Toast.makeText(
                            this@MainActivity,
                            "Seçilen klasörde aktarılabilecek dosya bulunamadı.",
                            Toast.LENGTH_SHORT
                        ).show()
                    } else {
                        selectedTransferItems.addAll(folderItems)
                        refreshSelectionUi()
                        Toast.makeText(
                            this@MainActivity,
                            "${folderItems.size} dosya klasörden sıfır önbellekle eklendi.",
                            Toast.LENGTH_SHORT
                        ).show()
                    }
                }
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // Allow direct file:// opening when testing %5 partial video files across API levels
        val vmBuilder = StrictMode.VmPolicy.Builder()
        StrictMode.setVmPolicy(vmBuilder.build())

        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        loadPreferences()
        setupSplashAndRadarViews()
        setupBottomNavigation()
        setupReceiveTabActions()
        setupSendTabActions()
        setupSettingsTabActions()
        setupEngines()

        requestNecessaryPermissionsIfNeeded()

        // Zero-Cache Share Sheet check on cold start
        handleIncomingShareIntent(intent)
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleIncomingShareIntent(intent)
    }

    private fun loadPreferences() {
        val prefs = getSharedPreferences("fast_transfer_prefs", Context.MODE_PRIVATE)
        deviceAlias = prefs.getString("device_alias", "Kind Carrot") ?: "Kind Carrot"
        val savedFavs = prefs.getStringSet("favorite_ips", emptySet()) ?: emptySet()
        favoriteIps.clear()
        favoriteIps.addAll(savedFavs)

        binding.tvDeviceAliasReceive.text = deviceAlias
        binding.etSettingsDeviceAlias.setText(deviceAlias)
    }

    private fun savePreferences() {
        val prefs = getSharedPreferences("fast_transfer_prefs", Context.MODE_PRIVATE)
        prefs.edit()
            .putString("device_alias", deviceAlias)
            .putStringSet("favorite_ips", favoriteIps)
            .apply()
    }

    /**
     * Replicates the exact video opening sequence (00:00 - 00:03):
     * Splash overlay shows teal segmented ring (#00A884) on pure black (#000000),
     * then transitions smoothly into the Receive ("Alım") screen with light cyan ring (#6CD4FF).
     */
    private fun setupSplashAndRadarViews() {
        binding.radarPulseSplash.setThemeColor(Color.parseColor("#00A884"))
        binding.radarPulseReceive.setThemeColor(Color.parseColor("#6CD4FF"))

        lifecycleScope.launch {
            delay(1100L)
            binding.splashOverlay.animate()
                .alpha(0f)
                .setDuration(350L)
                .withEndAction {
                    binding.splashOverlay.visibility = View.GONE
                }
                .start()
        }

        // Cycle skeleton device icon (Phone -> Laptop -> Terminal -> Server) and tip text
        // exactly as seen in Video 00:04 -> 00:09 -> 00:14 -> 00:17
        skeletonAnimationJob = lifecycleScope.launch {
            val icons = listOf(
                R.drawable.ic_device_phone,
                R.drawable.ic_device_laptop,
                R.drawable.ic_device_terminal,
                R.drawable.ic_device_server
            )
            var step = 0
            while (isActive) {
                binding.ivSkeletonDeviceIcon.setImageResource(icons[step % icons.size])
                binding.tvDynamicTip.setText(
                    if (step % 2 == 0) R.string.tip_same_wifi else R.string.tip_share_sheet
                )
                step++
                delay(3200L)
            }
        }
    }

    private fun setupBottomNavigation() {
        binding.navItemReceive.setOnClickListener { switchTab(0) }
        binding.navItemSend.setOnClickListener { switchTab(1) }
        binding.navItemSettings.setOnClickListener { switchTab(2) }
    }

    private fun switchTab(index: Int) {
        binding.pageReceive.visibility = if (index == 0) View.VISIBLE else View.GONE
        binding.pageSend.visibility = if (index == 1) View.VISIBLE else View.GONE
        binding.pageSettings.visibility = if (index == 2) View.VISIBLE else View.GONE

        val white = ContextCompat.getColor(this, R.color.text_primary_white)
        val muted = ContextCompat.getColor(this, R.color.text_secondary_muted)

        // Tab 0: Alım
        binding.pillReceive.setBackgroundResource(if (index == 0) R.drawable.bg_nav_pill else 0)
        binding.ivNavReceive.setColorFilter(if (index == 0) white else muted)
        binding.tvNavReceive.setTextColor(if (index == 0) white else muted)

        // Tab 1: Gönder
        binding.pillSend.setBackgroundResource(if (index == 1) R.drawable.bg_nav_pill else 0)
        binding.ivNavSend.setColorFilter(if (index == 1) white else muted)
        binding.tvNavSend.setTextColor(if (index == 1) white else muted)

        // Tab 2: Ayarlar
        binding.pillSettings.setBackgroundResource(if (index == 2) R.drawable.bg_nav_pill else 0)
        binding.ivNavSettings.setColorFilter(if (index == 2) white else muted)
        binding.tvNavSettings.setTextColor(if (index == 2) white else muted)
    }

    private fun setupReceiveTabActions() {
        binding.btnReceiveHistory.setOnClickListener {
            showTransferHistoryDialog()
        }
        binding.btnReceiveInfo.setOnClickListener {
            showNetworkAndDeviceInfoDialog()
        }
        binding.btnReceiveViaLink.setOnClickListener {
            showWebLinkInfoDialog()
        }
    }

    private fun setupSendTabActions() {
        nearbyAdapter = NearbyDeviceAdapter { peer ->
            initiateTransferToPeer(peer)
        }
        binding.rvNearbyDevices.layoutManager = LinearLayoutManager(this)
        binding.rvNearbyDevices.adapter = nearbyAdapter

        // 6 Primary Selection Cards (Dosya, Medya, Yapıştır, Metin, Klasör, Uygulama)
        binding.btnSelectFile.setOnClickListener {
            pickFilesLauncher.launch(arrayOf("*/*"))
        }
        binding.btnSelectMedia.setOnClickListener {
            pickMediaLauncher.launch("*/*")
        }
        binding.btnSelectPaste.setOnClickListener {
            handleClipboardPasteSelection()
        }
        binding.btnSelectText.setOnClickListener {
            showTextInputDialog()
        }
        binding.btnSelectFolder.setOnClickListener {
            pickFolderLauncher.launch(null)
        }
        binding.btnSelectApp.setOnClickListener {
            showApkPickerDialog()
        }

        // Selected Summary Card Actions (Video 00:16 - 00:17)
        binding.btnClearSelection.setOnClickListener {
            selectedTransferItems.clear()
            refreshSelectionUi()
        }
        binding.btnEditSelection.setOnClickListener {
            showSelectedFilesEditDialog()
        }
        binding.btnAddMoreSelection.setOnClickListener {
            showAddSelectionBottomSheet()
        }

        // Nearby Devices Header Actions (Refresh, Manual IP Scan, Favorites, Send Mode Menu)
        binding.btnRefreshPeers.setOnClickListener {
            it.animate().rotationBy(360f).setDuration(450L).start()
            wifiDirectManager.discoverPeers()
            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
            Toast.makeText(this, "5 GHz / 2.4 GHz Cihaz Taraması Yenilendi", Toast.LENGTH_SHORT).show()
        }
        binding.btnManualScanIp.setOnClickListener {
            showManualIpConnectDialog()
        }
        binding.btnFavoritesDialog.setOnClickListener {
            showFavoritesDialog()
        }
        binding.btnSendModeMenu.setOnClickListener { anchor ->
            showSendModePopupMenu(anchor)
        }

        binding.btnTroubleshoot.setOnClickListener {
            showTroubleshootDialog()
        }

        // Live Transfer Controls (Pause/Resume, Cancel & Preserve %5 Partial, Open Partial File)
        binding.btnPauseResumeTransfer.setOnClickListener {
            val paused = transferEngine.togglePauseResume()
            binding.btnPauseResumeTransfer.text = if (paused) "Devam Et" else "Duraklat"
        }

        binding.btnCancelTransferKeepPartial.setOnClickListener {
            transferEngine.cancelAndPreservePartialFile()
            Toast.makeText(
                this,
                "Transfer durduruldu! O ana kadar aktarılan kısmi dosya silinmedi ve mühürlendi.",
                Toast.LENGTH_LONG
            ).show()
        }

        binding.btnOpenPartialFile.setOnClickListener {
            val path = lastSavedOrPartialFilePath
            if (!path.isNullOrEmpty()) {
                openFileInExternalPlayer(File(path))
            }
        }
    }

    private fun setupSettingsTabActions() {
        binding.btnSaveAlias.setOnClickListener {
            val newAlias = binding.etSettingsDeviceAlias.text.toString().trim()
            if (newAlias.isNotEmpty()) {
                deviceAlias = newAlias
                binding.tvDeviceAliasReceive.text = deviceAlias
                savePreferences()
                lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                Toast.makeText(this, "Cihaz adı güncellendi: $deviceAlias", Toast.LENGTH_SHORT).show()
            }
        }

        binding.switchPrefer5Ghz.setOnCheckedChangeListener { _, isChecked ->
            wifiDirectManager.preferHighBand5Ghz = isChecked
            val modeText = if (isChecked) {
                "Önce 6 GHz / 5 GHz (Desteklenmezse 0ms 2.4 GHz Fallback)"
            } else {
                "Doğrudan 2.4 GHz Maksimum Menzil / Eski Cihaz Uyumluluk Modu"
            }
            Toast.makeText(this, modeText, Toast.LENGTH_SHORT).show()
        }
    }

    private fun setupEngines() {
        // Start Foreground Service with PARTIAL_WAKE_LOCK & WIFI_MODE_FULL_LOW_LATENCY
        val serviceIntent = Intent(this, TransferForegroundService::class.java).apply {
            action = TransferForegroundService.ACTION_START_ENGINE
        }
        CompatibilityHelper.startForegroundServiceCompat(this, serviceIntent)

        // Initialize Zero-Copy NIO Transfer Engine
        transferEngine = ZeroCopyTransferEngine(
            context = this,
            onProgressUpdate = { progress ->
                runOnUiThread {
                    renderTransferProgress(progress)
                }
            },
            onHistoryAdded = { record ->
                runOnUiThread {
                    lastSavedOrPartialFilePath = record.filePath
                }
            }
        )
        transferEngine.startServer()

        // Initialize Cascading Band Wi-Fi Direct Manager (6GHz/5GHz -> 2.4GHz Fallback)
        wifiDirectManager = CascadingWifiDirectManager(
            context = this,
            onPeersUpdated = { peers ->
                runOnUiThread {
                    p2pPeers.clear()
                    p2pPeers.addAll(peers)
                    mergeAndRenderDiscoveredPeers()
                }
            },
            onDirectConnectionReady = { groupOwnerIp, isGroupOwner, bandLabel ->
                runOnUiThread {
                    binding.tvReceiveBandStatus.text = "$bandLabel • Grup Hazır ($groupOwnerIp)"
                    lanDiscoveryManager.sendUnicastAnnounce(groupOwnerIp)
                    val pending = pendingPeerForDirectTransfer
                    if (pending != null && selectedTransferItems.isNotEmpty() && !isGroupOwner) {
                        pendingPeerForDirectTransfer = null
                        transferEngine.sendItemsToPeers(
                            items = selectedTransferItems.toList(),
                            targetHostIps = listOf(groupOwnerIp to 53317),
                            transportLabel = bandLabel
                        )
                    }
                }
            },
            onStatusLog = { statusText ->
                runOnUiThread {
                    binding.tvReceiveBandStatus.text = "$statusText • Port 53317"
                }
            }
        )
        wifiDirectManager.start()

        // Initialize LAN / Hotspot NSD + UDP Discovery Manager
        lanDiscoveryManager = LanDiscoveryManager(
            context = this,
            getLocalAlias = { deviceAlias },
            onLanPeersChanged = { peers ->
                runOnUiThread {
                    lanPeers.clear()
                    lanPeers.addAll(peers)
                    mergeAndRenderDiscoveredPeers()
                }
            }
        )
        lanDiscoveryManager.start()

        // Restore saved favorite IPs into discovery list
        for (favIp in favoriteIps) {
            lanDiscoveryManager.addManualFavoritePeer(favIp)
        }
    }

    /**
     * ZERO-CACHE SHARE SHEET HANDLER:
     * Triggered when user selects any file/video in Gallery or File Manager and taps "Share -> FastTransfer".
     * Never copies the file to cacheDir! Populates the selection list in <1ms and switches to Gönder tab.
     */
    private fun handleIncomingShareIntent(intent: Intent?) {
        val sharedItems = StorageHelper.extractTransferItemsFromIntent(this, intent)
        if (sharedItems.isNotEmpty()) {
            selectedTransferItems.addAll(sharedItems)
            refreshSelectionUi()
            switchTab(1) // Switch directly to Gönder tab
            Toast.makeText(
                this,
                "${sharedItems.size} dosya önbelleğe kopyalanmadan (Zero-Cache) doğrudan akışa hazır!",
                Toast.LENGTH_SHORT
            ).show()
        }
    }

    private fun refreshSelectionUi() {
        transferEngine.setItemsForWebLinkShare(selectedTransferItems.toList())
        if (selectedTransferItems.isEmpty()) {
            binding.layoutSelectionGrid.visibility = View.VISIBLE
            binding.layoutSelectedSummary.visibility = View.GONE
        } else {
            binding.layoutSelectionGrid.visibility = View.GONE
            binding.layoutSelectedSummary.visibility = View.VISIBLE

            val totalCount = selectedTransferItems.size
            val totalBytes = selectedTransferItems.sumOf { it.sizeBytes }
            binding.tvSelectedCount.text = "Dosyalar: $totalCount"
            binding.tvSelectedSize.text = "Boyut: ${StorageHelper.formatFileSize(totalBytes)}"
            binding.tvSelectedFileNames.text = selectedTransferItems.joinToString(", ") { it.name }
        }
    }

    private fun mergeAndRenderDiscoveredPeers() {
        val combined = mutableListOf<PeerDevice>()
        combined.addAll(lanPeers)
        for (p2p in p2pPeers) {
            if (combined.none { it.alias.equals(p2p.alias, ignoreCase = true) }) {
                combined.add(p2p)
            }
        }

        if (combined.isEmpty()) {
            binding.cardScanningPlaceholder.visibility = View.VISIBLE
            binding.rvNearbyDevices.visibility = View.GONE
        } else {
            binding.cardScanningPlaceholder.visibility = View.GONE
            binding.rvNearbyDevices.visibility = View.VISIBLE
            nearbyAdapter.submitList(combined)
        }
    }

    private fun initiateTransferToPeer(peer: PeerDevice) {
        if (selectedTransferItems.isEmpty()) {
            Toast.makeText(
                this,
                "Lütfen önce gönderilecek dosya, medya, metin veya uygulama seçin.",
                Toast.LENGTH_SHORT
            ).show()
            return
        }

        if (currentSendMode == SendMode.SHARE_VIA_LINK) {
            showWebLinkInfoDialog()
            return
        }

        if (currentSendMode == SendMode.MULTI_RECIPIENT) {
            val allTargets = lanPeers.mapNotNull { p -> p.ipAddress?.let { it to p.port } }.distinct()
            if (allTargets.isNotEmpty()) {
                transferEngine.sendItemsToPeers(
                    items = selectedTransferItems.toList(),
                    targetHostIps = allTargets,
                    transportLabel = "Çoklu Alıcı Zero-Copy (${allTargets.size} Cihaz)"
                )
                return
            }
        }

        // If peer already has a resolved LAN / Hotspot / Wi-Fi Direct IP, stream immediately!
        if (!peer.ipAddress.isNullOrBlank()) {
            transferEngine.sendItemsToPeers(
                items = selectedTransferItems.toList(),
                targetHostIps = listOf(peer.ipAddress to peer.port),
                transportLabel = peer.transport.label
            )
            return
        }

        // Otherwise negotiate Wi-Fi Direct with Cascading Band Fallback (6GHz/5GHz -> 2.4GHz)
        if (!peer.macAddress.isNullOrBlank()) {
            pendingPeerForDirectTransfer = peer
            Toast.makeText(
                this,
                "${peer.alias} ile maksimum hız kanalı (5 GHz -> 2.4 GHz Fallback) kuruluyor...",
                Toast.LENGTH_SHORT
            ).show()
            wifiDirectManager.connectWithCascadingBandFallback(peer.macAddress) { success, msg ->
                if (!success) {
                    Toast.makeText(this, "Wi-Fi Direct: $msg", Toast.LENGTH_SHORT).show()
                }
            }
        }
    }

    private fun renderTransferProgress(progress: TransferProgress) {
        binding.cardActiveTransfer.visibility = View.VISIBLE
        binding.tvTransferTitle.text = "Zero-Copy Aktarım (${progress.transportLabel})"
        binding.tvTransferFileName.text = progress.fileName
        binding.progressBarTransfer.progress = progress.progressPercent

        val speedFormat = DecimalFormat("0.0")
        binding.tvTransferSpeedMbps.text = "${speedFormat.format(progress.speedMbps)} MB/s"

        val transferredStr = StorageHelper.formatFileSize(progress.bytesTransferred)
        val totalStr = StorageHelper.formatFileSize(progress.totalBytes)
        binding.tvTransferBytesAndPercent.text = "%${progress.progressPercent} ($transferredStr / $totalStr)"

        when (progress.status) {
            TransferStatus.CONNECTING -> {
                binding.tvTransferEta.text = progress.message ?: "Bağlanıyor..."
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.TRANSFERRING -> {
                binding.tvTransferEta.text = "Kalan Süre: ${formatEta(progress.etaSeconds)}"
                binding.btnPauseResumeTransfer.visibility = View.VISIBLE
                binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
                if (!progress.savedFilePath.isNullOrEmpty()) {
                    lastSavedOrPartialFilePath = progress.savedFilePath
                }
            }
            TransferStatus.PAUSED -> {
                binding.tvTransferEta.text = "Duraklatıldı (Kısmi veri koruma altında)"
                if (!progress.savedFilePath.isNullOrEmpty()) {
                    lastSavedOrPartialFilePath = progress.savedFilePath
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                }
            }
            TransferStatus.PARTIAL_PRESERVED -> {
                binding.tvTransferEta.text = progress.message ?: "Kısmi dosya korundu ve mühürlendi!"
                if (!progress.savedFilePath.isNullOrEmpty()) {
                    lastSavedOrPartialFilePath = progress.savedFilePath
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                }
            }
            TransferStatus.COMPLETED -> {
                binding.tvTransferEta.text = progress.message ?: "Aktarım Tamamlandı"
                if (!progress.savedFilePath.isNullOrEmpty()) {
                    lastSavedOrPartialFilePath = progress.savedFilePath
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                    binding.btnOpenPartialFile.text = "Dosyayı Aç / Oynat"
                }
                if (currentSendMode == SendMode.SINGLE_RECIPIENT) {
                    // Video 00:36: "Tek alıcı" clears selection automatically after transfer finishes
                    selectedTransferItems.clear()
                    refreshSelectionUi()
                }
            }
            TransferStatus.ERROR -> {
                binding.tvTransferEta.text = progress.message ?: "Bağlantı hatası"
            }
            TransferStatus.IDLE -> {}
        }
    }

    private fun formatEta(seconds: Long): String {
        if (seconds <= 0L) return "< 1 sn"
        val mins = seconds / 60L
        val secs = seconds % 60L
        return if (mins > 0L) "${mins} dk ${secs} sn" else "${secs} sn"
    }

    private fun handleClipboardPasteSelection() {
        val clipItem = StorageHelper.readClipboardAsTransferItem(this)
        if (clipItem != null) {
            selectedTransferItems.add(clipItem)
            refreshSelectionUi()
            Toast.makeText(this, "Panodaki içerik eklendi: ${clipItem.name}", Toast.LENGTH_SHORT).show()
        } else {
            Toast.makeText(this, "Panoda kopyalanmış metin veya dosya bulunamadı.", Toast.LENGTH_SHORT).show()
        }
    }

    /**
     * Exact replica of Video 00:19: "Mesaj yazınız" dialog with outlined input, "İptal et" and cyan "Onayla" pill.
     */
    private fun showTextInputDialog() {
        val dialog = Dialog(this)
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE)
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_text_input, null)
        dialog.setContentView(view)
        dialog.window?.setBackgroundDrawable(ColorDrawable(Color.TRANSPARENT))
        dialog.window?.setLayout(
            (resources.displayMetrics.widthPixels * 0.88).toInt(),
            ViewGroup.LayoutParams.WRAP_CONTENT
        )

        val etInput = view.findViewById<EditText>(R.id.etMessageInput)
        val btnCancel = view.findViewById<TextView>(R.id.btnCancelTextDialog)
        val btnConfirm = view.findViewById<TextView>(R.id.btnConfirmTextDialog)

        btnCancel.setOnClickListener { dialog.dismiss() }
        btnConfirm.setOnClickListener {
            val text = etInput.text.toString()
            if (text.isNotEmpty()) {
                val item = StorageHelper.buildTransferItemFromText(text)
                selectedTransferItems.add(item)
                refreshSelectionUi()
            }
            dialog.dismiss()
        }
        dialog.show()
    }

    /**
     * Exact replica of Video 00:25 - 00:28: "Uygulamalar (APK)" full picker with search,
     * "X Uygulamalar" counter, and "Select Multiple Apps" switch.
     */
    private fun showApkPickerDialog() {
        val dialog = Dialog(this, android.R.style.Theme_Black_NoTitleBar_Fullscreen)
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_apk_picker, null)
        dialog.setContentView(view)

        val btnClose = view.findViewById<ImageButton>(R.id.btnCloseApkDialog)
        val etSearch = view.findViewById<EditText>(R.id.etSearchApk)
        val tvCount = view.findViewById<TextView>(R.id.tvApkTotalCount)
        val switchMulti = view.findViewById<SwitchMaterial>(R.id.switchSelectMultipleApks)
        val progress = view.findViewById<ProgressBar>(R.id.progressApkLoading)
        val rvApks = view.findViewById<RecyclerView>(R.id.rvApkList)
        val btnConfirmMulti = view.findViewById<TextView>(R.id.btnConfirmApkSelection)

        lateinit var apkAdapter: ApkListAdapter
        apkAdapter = ApkListAdapter(
            onSingleAppSelected = { appItem ->
                addApkAppToSelection(appItem)
                refreshSelectionUi()
                dialog.dismiss()
            },
            onMultiSelectionChanged = { selectedList ->
                btnConfirmMulti.visibility = if (selectedList.isNotEmpty()) View.VISIBLE else View.GONE
                btnConfirmMulti.text = "Onayla (${selectedList.size} Uygulama)"
            }
        )

        rvApks.layoutManager = LinearLayoutManager(this)
        rvApks.adapter = apkAdapter

        switchMulti.setOnCheckedChangeListener { _, isChecked ->
            apkAdapter.isMultiSelectMode = isChecked
            btnConfirmMulti.visibility =
                if (isChecked && apkAdapter.getSelectedApps().isNotEmpty()) View.VISIBLE else View.GONE
        }

        etSearch.addTextChangedListener(object : TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, start: Int, count: Int, after: Int) {}
            override fun onTextChanged(s: CharSequence?, start: Int, before: Int, count: Int) {
                apkAdapter.filter(s?.toString() ?: "")
                tvCount.text = "${apkAdapter.getFilteredCount()} Uygulamalar"
            }
            override fun afterTextChanged(s: Editable?) {}
        })

        btnConfirmMulti.setOnClickListener {
            val chosen = apkAdapter.getSelectedApps()
            for (app in chosen) {
                addApkAppToSelection(app)
            }
            refreshSelectionUi()
            dialog.dismiss()
        }

        btnClose.setOnClickListener { dialog.dismiss() }

        lifecycleScope.launch(Dispatchers.IO) {
            val installedApks = StorageHelper.loadInstalledApks(this@MainActivity)
            withContext(Dispatchers.Main) {
                progress.visibility = View.GONE
                apkAdapter.setApps(installedApks)
                tvCount.text = "${installedApks.size} Uygulamalar"
            }
        }

        dialog.show()
    }

    private fun addApkAppToSelection(app: ApkAppItem) {
        val cleanFileName = StorageHelper.sanitizeFileName("${app.appName}_v${app.versionName}.apk")
        selectedTransferItems.add(
            TransferItem(
                id = UUID.randomUUID().toString(),
                name = cleanFileName,
                sizeBytes = app.sizeBytes,
                mimeType = "application/vnd.android.package-archive",
                uri = Uri.fromFile(app.apkFile),
                directFile = app.apkFile
            )
        )
    }

    /**
     * Exact replica of Video 00:31 - 00:32: "Seçime ekle" / "Ne eklemek istiyorsunuz ?" bottom sheet
     * with 6 bright cyan (#6CD4FF) cards.
     */
    private fun showAddSelectionBottomSheet() {
        val sheet = BottomSheetDialog(this)
        val view = LayoutInflater.from(this).inflate(R.layout.bottom_sheet_add_selection, null)
        sheet.setContentView(view)

        view.findViewById<View>(R.id.sheetBtnFile).setOnClickListener {
            sheet.dismiss()
            pickFilesLauncher.launch(arrayOf("*/*"))
        }
        view.findViewById<View>(R.id.sheetBtnMedia).setOnClickListener {
            sheet.dismiss()
            pickMediaLauncher.launch("*/*")
        }
        view.findViewById<View>(R.id.sheetBtnPaste).setOnClickListener {
            sheet.dismiss()
            handleClipboardPasteSelection()
        }
        view.findViewById<View>(R.id.sheetBtnText).setOnClickListener {
            sheet.dismiss()
            showTextInputDialog()
        }
        view.findViewById<View>(R.id.sheetBtnFolder).setOnClickListener {
            sheet.dismiss()
            pickFolderLauncher.launch(null)
        }
        view.findViewById<View>(R.id.sheetBtnApp).setOnClickListener {
            sheet.dismiss()
            showApkPickerDialog()
        }

        sheet.show()
    }

    /**
     * Exact replica of Video 00:34: "Favoriler" / "Henüz favori cihaz yok." dialog with "İptal et" and "Ekle".
     */
    private fun showFavoritesDialog() {
        val dialog = Dialog(this)
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE)
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_favorites, null)
        dialog.setContentView(view)
        dialog.window?.setBackgroundDrawable(ColorDrawable(Color.TRANSPARENT))
        dialog.window?.setLayout(
            (resources.displayMetrics.widthPixels * 0.86).toInt(),
            ViewGroup.LayoutParams.WRAP_CONTENT
        )

        val tvStatus = view.findViewById<TextView>(R.id.tvFavoritesListOrEmpty)
        val etIp = view.findViewById<EditText>(R.id.etAddFavoriteIp)
        val btnCancel = view.findViewById<TextView>(R.id.btnCloseFavorites)
        val btnAdd = view.findViewById<TextView>(R.id.btnAddFavoriteAction)

        fun updateFavText() {
            if (favoriteIps.isEmpty()) {
                tvStatus.text = getString(R.string.dialog_favorites_empty)
            } else {
                tvStatus.text = favoriteIps.joinToString("\n") { "• $it (Port 53317)" }
            }
        }
        updateFavText()

        btnCancel.setOnClickListener { dialog.dismiss() }
        btnAdd.setOnClickListener {
            if (etIp.visibility == View.GONE) {
                etIp.visibility = View.VISIBLE
                etIp.requestFocus()
                btnAdd.text = getString(R.string.btn_confirm)
            } else {
                val ip = etIp.text.toString().trim()
                if (ip.isNotEmpty()) {
                    favoriteIps.add(ip)
                    savePreferences()
                    lanDiscoveryManager.addManualFavoritePeer(ip)
                    dialog.dismiss()
                }
            }
        }

        dialog.show()
    }

    /**
     * Exact replica of Video 00:36 - 00:37: Gear popup menu with
     * "Tek alıcı", "Birden fazla alıcı", "Link üzerinden paylaşın", and "? Açıklama".
     */
    private fun showSendModePopupMenu(anchor: View) {
        val popupView = LayoutInflater.from(this).inflate(R.layout.popup_send_mode, null)
        val popupWindow = PopupWindow(
            popupView,
            ViewGroup.LayoutParams.WRAP_CONTENT,
            ViewGroup.LayoutParams.WRAP_CONTENT,
            true
        ).apply {
            elevation = 16f
            setBackgroundDrawable(ColorDrawable(Color.TRANSPARENT))
        }

        val ivSingle = popupView.findViewById<ImageView>(R.id.ivCheckSingle)
        val ivMulti = popupView.findViewById<ImageView>(R.id.ivCheckMulti)
        val ivLink = popupView.findViewById<ImageView>(R.id.ivCheckLink)

        ivSingle.visibility = if (currentSendMode == SendMode.SINGLE_RECIPIENT) View.VISIBLE else View.INVISIBLE
        ivMulti.visibility = if (currentSendMode == SendMode.MULTI_RECIPIENT) View.VISIBLE else View.INVISIBLE
        ivLink.visibility = if (currentSendMode == SendMode.SHARE_VIA_LINK) View.VISIBLE else View.INVISIBLE

        popupView.findViewById<View>(R.id.menuSingleRecipient).setOnClickListener {
            currentSendMode = SendMode.SINGLE_RECIPIENT
            popupWindow.dismiss()
        }
        popupView.findViewById<View>(R.id.menuMultiRecipient).setOnClickListener {
            currentSendMode = SendMode.MULTI_RECIPIENT
            popupWindow.dismiss()
        }
        popupView.findViewById<View>(R.id.menuShareViaLink).setOnClickListener {
            currentSendMode = SendMode.SHARE_VIA_LINK
            popupWindow.dismiss()
            showWebLinkInfoDialog()
        }
        popupView.findViewById<View>(R.id.menuModeExplanation).setOnClickListener {
            popupWindow.dismiss()
            AlertDialog.Builder(this)
                .setTitle("Gönderim Modları")
                .setMessage("""• Tek alıcı: Dosyalar tek bir hedef cihaza gönderilir ve aktarım sonunda seçim temizlenir.\n\n• Birden fazla alıcı: Seçiminiz korunur ve birden fazla cihaza art arda gönderilir.\n\n• Link üzerinden paylaşın: Uygulama yüklü olmayan cihazlar tarayıcıdan (HTTP Port 53319) doğrudan indirebilir.""".trimIndent())
                .setPositiveButton("Tamam", null)
                .show()
        }

        popupWindow.showAsDropDown(anchor, -160, 8, Gravity.END)
    }

    private fun showManualIpConnectDialog() {
        val dialog = Dialog(this)
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE)
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_text_input, null)
        dialog.setContentView(view)
        dialog.window?.setBackgroundDrawable(ColorDrawable(Color.TRANSPARENT))
        dialog.window?.setLayout(
            (resources.displayMetrics.widthPixels * 0.88).toInt(),
            ViewGroup.LayoutParams.WRAP_CONTENT
        )

        val etInput = view.findViewById<EditText>(R.id.etMessageInput)
        etInput.hint = "Hedef IP Adresi (Örn: 192.168.49.1 veya 192.168.1.45)"
        view.findViewById<TextView>(R.id.btnCancelTextDialog).setOnClickListener { dialog.dismiss() }
        view.findViewById<TextView>(R.id.btnConfirmTextDialog).setOnClickListener {
            val ip = etInput.text.toString().trim()
            if (ip.isNotEmpty()) {
                lanDiscoveryManager.addManualFavoritePeer(ip, "Doğrudan Cihaz ($ip)")
                if (selectedTransferItems.isNotEmpty()) {
                    transferEngine.sendItemsToPeers(
                        items = selectedTransferItems.toList(),
                        targetHostIps = listOf(ip to 53317),
                        transportLabel = "Doğrudan IP (Zero-Copy)"
                    )
                }
            }
            dialog.dismiss()
        }
        dialog.show()
    }

    private fun showSelectedFilesEditDialog() {
        if (selectedTransferItems.isEmpty()) return
        val names = selectedTransferItems.map { "${it.name} (${StorageHelper.formatFileSize(it.sizeBytes)})" }.toTypedArray()
        AlertDialog.Builder(this)
            .setTitle("Seçili Dosyalar (Silmek için dokunun)")
            .setItems(names) { dialog: android.content.DialogInterface, which: Int ->
                if (which in selectedTransferItems.indices) {
                    selectedTransferItems.removeAt(which)
                    refreshSelectionUi()
                }
            }
            .setPositiveButton("Kapat", null)
            .show()
    }

    private fun showTransferHistoryDialog() {
        val records = transferEngine.historyRecords.toList()
        if (records.isEmpty()) {
            AlertDialog.Builder(this)
                .setTitle("Aktarım Geçmişi (%5 Kısmi & Tam Dosyalar)")
                .setMessage("Henüz alınan dosya yok. Alınan tam veya %5 kısmi dosyalar doğrudan Download klasörüne kaydedilir ve buradan tek dokunuşla açılabilir.")
                .setPositiveButton("Tamam", null)
                .show()
            return
        }

        val labels = records.map { rec ->
            val statusTag = if (rec.isPartial) "[KISMİ KORUNDU]" else "[TAMAMLANDI]"
            "$statusTag ${rec.fileName} (${StorageHelper.formatFileSize(rec.bytesWritten)})"
        }.toTypedArray()

        AlertDialog.Builder(this)
            .setTitle("Alınan Dosyalar (Oynatmak için seçin)")
            .setItems(labels) { dialog: android.content.DialogInterface, idx: Int ->
                val rec = records[idx]
                openFileInExternalPlayer(File(rec.filePath))
            }
            .setPositiveButton("Kapat", null)
            .show()
    }

    private fun showNetworkAndDeviceInfoDialog() {
        val ips = lanDiscoveryManager.getLocalIpAddresses()
        val ipText = if (ips.isEmpty()) "Wi-Fi Direct Bekleniyor (192.168.49.1)" else ips.joinToString(", ")
        val hwBands = wifiDirectManager.getHardwareBandCapabilitySummary()

        AlertDialog.Builder(this)
            .setTitle("Donanım & Ağ Durumu")
            .setMessage("""• Cihaz Adı: $deviceAlias\n• Desteklenen Bantlar: $hwBands\n• Yerel IP Adresleri: $ipText\n• Zero-Copy TCP Portu: 53317 (8 MB Tampon)\n• Tarayıcı Link Portu: 53319\n• Hedef Klasör: /storage/emulated/0/Download""".trimIndent())
            .setPositiveButton("Tamam", null)
            .show()
    }

    private fun showWebLinkInfoDialog() {
        val ips = lanDiscoveryManager.getLocalIpAddresses()
        val primaryIp = ips.firstOrNull() ?: "192.168.49.1"
        val url = "http://$primaryIp:53319"

        AlertDialog.Builder(this)
            .setTitle("Link Üzerinden Al / Paylaş")
            .setMessage("""Karşı cihazda uygulama yüklü olmasa bile aynı Wi-Fi veya Hotspot ağına bağlanıp tarayıcıdan şu adrese girerek dosyaları doğrudan indirebilirsiniz:\n\n$url""".trimIndent())
            .setPositiveButton("Tamam", null)
            .show()
    }

    private fun showTroubleshootDialog() {
        val hwBands = wifiDirectManager.getHardwareBandCapabilitySummary()
        val ips = lanDiscoveryManager.getLocalIpAddresses().joinToString(", ").ifEmpty { "Yok" }
        AlertDialog.Builder(this)
            .setTitle("Sorun Giderme ve Kademeli Frekans Onarımı")
            .setMessage("""• Donanım Bant Desteği: $hwBands\n• Aktif IP Adresleri: $ips\n\nEğer karşıdaki cihaz eski bir modelse ve yalnızca 2.4 GHz destekliyorsa, uygulama 5 GHz denemesinden 0ms içinde otomatik olarak 2.4 GHz moduna düşer.\n\nTüm ağ soketlerini ve Wi-Fi Direct / LAN keşfini şimdi yeniden başlatmak ister misiniz?""".trimIndent())
            .setPositiveButton("Taramayı & 2.4 GHz Uyumunu Yenile") { dialog: android.content.DialogInterface, which: Int ->
                wifiDirectManager.discoverPeers()
                lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                Toast.makeText(this, "Tüm Wi-Fi Direct ve LAN kanalları yenilendi.", Toast.LENGTH_SHORT).show()
            }
            .setNegativeButton("Kapat", null)
            .show()
    }

    private fun openFileInExternalPlayer(file: File) {
        if (!file.exists()) {
            Toast.makeText(this, "Dosya bulunamadı: ${file.absolutePath}", Toast.LENGTH_SHORT).show()
            return
        }
        try {
            val mime = StorageHelper.guessMimeType(file.name)
            val intent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(Uri.fromFile(file), mime)
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(Intent.createChooser(intent, "Kısmi / Tam Dosyayı Aç (${file.name})"))
        } catch (t: Throwable) {
            Toast.makeText(
                this,
                "Dosya konumu: ${file.absolutePath}",
                Toast.LENGTH_LONG
            ).show()
        }
    }

    private fun requestNecessaryPermissionsIfNeeded() {
        if (!CompatibilityHelper.hasAllRequiredPermissions(this)) {
            permissionLauncher.launch(CompatibilityHelper.getRequiredRuntimePermissions())
        }
    }

    override fun onDestroy() {
        skeletonAnimationJob?.cancel()
        wifiDirectManager.stop()
        lanDiscoveryManager.stop()
        transferEngine.stop()
        super.onDestroy()
    }
}
