package com.fasttransfer.p2p

import android.app.Dialog
import android.content.Context
import android.content.DialogInterface
import android.content.Intent
import android.graphics.Color
import android.graphics.drawable.ColorDrawable
import android.net.Uri
import android.os.Build
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
import androidx.core.content.FileProvider
import androidx.lifecycle.lifecycleScope
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import com.fasttransfer.p2p.databinding.ActivityMainBinding
import com.fasttransfer.p2p.model.ApkAppItem
import com.fasttransfer.p2p.model.IncomingTransferRequest
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
    private var activeConsentDialog: Dialog? = null

    private var skeletonAnimationJob: Job? = null

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
        val autoAccept = prefs.getBoolean("auto_accept_incoming", false)
        val keepNotif = prefs.getBoolean("keep_service_notif", false)
        favoriteIps.clear()
        // Filter out any old ghost 192.168.49.1 entry that was saved in earlier versions
        for (ip in savedFavs) {
            if (ip != "192.168.49.1") {
                favoriteIps.add(ip)
            }
        }
        savePreferences()

        binding.tvDeviceAliasReceive.text = deviceAlias
        binding.etSettingsDeviceAlias.setText(deviceAlias)
        binding.switchAutoAccept.isChecked = autoAccept
        binding.switchKeepServiceNotification.isChecked = keepNotif
    }

    private fun savePreferences() {
        val prefs = getSharedPreferences("fast_transfer_prefs", Context.MODE_PRIVATE)
        prefs.edit()
            .putString("device_alias", deviceAlias)
            .putStringSet("favorite_ips", favoriteIps)
            .putBoolean("auto_accept_incoming", binding.switchAutoAccept.isChecked)
            .putBoolean("keep_service_notif", binding.switchKeepServiceNotification.isChecked)
            .apply()
    }

    private fun setHardwareServiceRunning(active: Boolean) {
        val intent = Intent(this, TransferForegroundService::class.java).apply {
            action = if (active) {
                TransferForegroundService.ACTION_START_ENGINE
            } else {
                TransferForegroundService.ACTION_STOP_ENGINE
            }
        }
        if (active) {
            CompatibilityHelper.startForegroundServiceCompat(this, intent)
        } else {
            try {
                startService(intent)
            } catch (_: Throwable) {
            }
            try {
                stopService(Intent(this, TransferForegroundService::class.java))
            } catch (_: Throwable) {
            }
        }
    }

    private fun setupSplashAndRadarViews() {
        binding.radarPulseSplash.setThemeColor(Color.parseColor("#00A884"))
        binding.radarPulseReceive.setThemeColor(Color.parseColor("#6CD4FF"))

        lifecycleScope.launch {
            delay(900L)
            binding.splashOverlay.animate()
                .alpha(0f)
                .setDuration(280L)
                .withEndAction {
                    binding.splashOverlay.visibility = View.GONE
                }
                .start()
        }

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

        binding.pillReceive.setBackgroundResource(if (index == 0) R.drawable.bg_nav_pill else 0)
        binding.ivNavReceive.setColorFilter(if (index == 0) white else muted)
        binding.tvNavReceive.setTextColor(if (index == 0) white else muted)

        binding.pillSend.setBackgroundResource(if (index == 1) R.drawable.bg_nav_pill else 0)
        binding.ivNavSend.setColorFilter(if (index == 1) white else muted)
        binding.tvNavSend.setTextColor(if (index == 1) white else muted)

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
        binding.btnCreate5GhzBridge.setOnClickListener {
            binding.tvCreate5GhzBridge.text = "5 GHz Doğrudan Köprü Hazırlanıyor..."
            wifiDirectManager.createDirect5GhzGroupBridge { success, msg, ssid, pass ->
                runOnUiThread {
                    binding.tvCreate5GhzBridge.text = if (success) {
                        "5 GHz Doğrudan Köprü Aktif (192.168.49.1)"
                    } else {
                        "5 GHz Doğrudan Köprü Kur (Modemsiz Maks Hız)"
                    }
                    binding.tvReceiveBandStatus.text = msg
                    lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)

                    if (success && !ssid.isNullOrEmpty()) {
                        val infoText = buildString {
                            appendLine("5 GHz Modemsiz Doğrudan Köprü Kuruldu!")
                            appendLine()
                            appendLine("• Wi-Fi Ağ Adı (SSID): $ssid")
                            appendLine("• Şifre: ${pass ?: "zeph12345"}")
                            appendLine("• Köprü IP Adresi: 192.168.49.1")
                            appendLine()
                            append("Gönderici telefondan Çevrimdışı (Modemsiz) bağlanmak için:")
                            appendLine()
                            appendLine("1) Gönderici telefonda Yakınınızdaki Cihazlar listesindeki Wi-Fi Direct kaydına dokunun, VEYA")
                            append("2) Gönderici telefonun Wi-Fi ayarlarından '$ssid' ağına bağlanın; cihazlar 192.168.49.1 üzerinden otomatik eşleşip maksimum 5 GHz hızda aktarım yapacaktır.")
                        }
                        AlertDialog.Builder(this)
                            .setTitle("5 GHz Doğrudan Köprü Hazır")
                            .setMessage(infoText)
                            .setPositiveButton("Tamam", null)
                            .show()
                    } else {
                        Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                    }
                }
            }
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

        binding.btnRefreshPeers.setOnClickListener {
            it.animate().rotationBy(360f).setDuration(450L).start()
            wifiDirectManager.discoverPeers()
            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
            Toast.makeText(this, "5 GHz Doğrudan Köprü & LAN Taraması Yenilendi", Toast.LENGTH_SHORT).show()
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

        binding.btnPauseResumeTransfer.setOnClickListener {
            val paused = transferEngine.togglePauseResume()
            binding.btnPauseResumeTransfer.text = if (paused) "Devam Et" else "Duraklat"
        }

        binding.btnCancelTransferKeepPartial.setOnClickListener {
            transferEngine.cancelAndPreservePartialFile()
            binding.btnPauseResumeTransfer.visibility = View.GONE
            binding.btnCancelTransferKeepPartial.visibility = View.GONE
            binding.tvTransferEta.text = "Durduruldu • Kısmi dosya korundu ve mühürlendi!"
            if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                binding.btnOpenPartialFile.visibility = View.VISIBLE
            }
            Toast.makeText(
                this,
                "İşlem anında durduruldu! İnen kısmi dosya korundu.",
                Toast.LENGTH_SHORT
            ).show()
        }

        binding.btnDismissTransferCard.setOnClickListener {
            transferEngine.cancelAndPreservePartialFile()
            activeConsentDialog?.dismiss()
            activeConsentDialog = null
            binding.cardActiveTransfer.visibility = View.GONE
            binding.btnPauseResumeTransfer.text = "Duraklat"
            binding.btnPauseResumeTransfer.visibility = View.VISIBLE
            binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
        }

        binding.btnOpenPartialFile.setOnClickListener {
            val path = lastSavedOrPartialFilePath
            if (!path.isNullOrEmpty()) {
                openFileInExternalPlayer(File(path))
            } else {
                showTransferHistoryDialog()
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
        }

        binding.switchAutoAccept.setOnCheckedChangeListener { _, isChecked ->
            if (::transferEngine.isInitialized) {
                transferEngine.autoAcceptIncoming = isChecked
            }
            savePreferences()
        }

        binding.switchKeepServiceNotification.setOnCheckedChangeListener { _, isChecked ->
            savePreferences()
            setHardwareServiceRunning(isChecked)
            val msg = if (isChecked) {
                "Üst çubuk bildirimi aktif edildi."
            } else {
                "Üst çubuk bildirimi kapatıldı (Yalnızca aktarım sırasında çalışır)."
            }
            Toast.makeText(this, msg, Toast.LENGTH_SHORT).show()
        }

        binding.btnStopServiceNotification.setOnClickListener {
            binding.switchKeepServiceNotification.isChecked = false
            savePreferences()
            setHardwareServiceRunning(false)
            Toast.makeText(this, "Üstteki bildirim ve arka plan servisi kapatıldı.", Toast.LENGTH_SHORT).show()
        }

        binding.btnClearHistorySettings.setOnClickListener {
            transferEngine.clearHistory()
            lastSavedOrPartialFilePath = null
            binding.cardActiveTransfer.visibility = View.GONE
            Toast.makeText(this, "Aktarım geçmişi ve ekran kartı temizlendi.", Toast.LENGTH_SHORT).show()
        }
    }

    private fun setupEngines() {
        // Only show persistent notification on startup if user explicitly enabled it in Settings!
        if (binding.switchKeepServiceNotification.isChecked) {
            setHardwareServiceRunning(true)
        } else {
            setHardwareServiceRunning(false)
        }

        transferEngine = ZeroCopyTransferEngine(
            context = this,
            getLocalAlias = { deviceAlias },
            onIncomingConsentRequest = { req ->
                runOnUiThread {
                    showIncomingConsentDialog(req)
                }
            },
            onProgressUpdate = { progress ->
                runOnUiThread {
                    renderTransferProgress(progress)
                }
            },
            onHistoryAdded = { record ->
                runOnUiThread {
                    lastSavedOrPartialFilePath = record.filePath
                }
            },
            onTransferActiveStateChanged = { isActiveTransfer ->
                runOnUiThread {
                    if (isActiveTransfer) {
                        setHardwareServiceRunning(true)
                    } else if (!binding.switchKeepServiceNotification.isChecked) {
                        setHardwareServiceRunning(false)
                    }
                }
            }
        )
        transferEngine.autoAcceptIncoming = binding.switchAutoAccept.isChecked
        transferEngine.startServer()

        wifiDirectManager = CascadingWifiDirectManager(
            context = this,
            onPeersUpdated = { peers ->
                runOnUiThread {
                    p2pPeers.clear()
                    p2pPeers.addAll(peers)
                    mergeAndRenderDiscoveredPeers()
                }
            },
            onDirectConnectionReady = { groupOwnerIp, isGroupOwner, bandLabel, ssid, _ ->
                runOnUiThread {
                    val ssidTag = if (!ssid.isNullOrEmpty()) " • $ssid" else ""
                    binding.tvReceiveBandStatus.text = "$bandLabel$ssidTag"
                    if (!isGroupOwner) {
                        // We are the P2P Client connected to the other phone's 5 GHz Group Owner bridge!
                        lanDiscoveryManager.sendUnicastAnnounce(groupOwnerIp)
                    } else {
                        lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                    }

                    val pending = pendingPeerForDirectTransfer
                    if (pending != null && selectedTransferItems.isNotEmpty()) {
                        pendingPeerForDirectTransfer = null
                        if (!isGroupOwner) {
                            transferEngine.sendItemsToPeers(
                                items = selectedTransferItems.toList(),
                                targetHostIps = listOf(groupOwnerIp to 53317),
                                transportLabel = bandLabel
                            )
                        } else {
                            val p2pClient = lanPeers.firstOrNull { it.ipAddress?.startsWith("192.168.49.") == true }
                            if (p2pClient?.ipAddress != null) {
                                transferEngine.sendItemsToPeers(
                                    items = selectedTransferItems.toList(),
                                    targetHostIps = listOf(p2pClient.ipAddress to 53317),
                                    transportLabel = bandLabel
                                )
                            } else {
                                binding.tvTransferEta.text = "Köprü kuruldu! Karşı cihazın bağlanması bekleniyor..."
                            }
                        }
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

        for (favIp in favoriteIps) {
            lanDiscoveryManager.addManualFavoritePeer(favIp)
        }
    }

    private fun showIncomingConsentDialog(req: IncomingTransferRequest) {
        activeConsentDialog?.dismiss()
        val dialog = Dialog(this)
        dialog.requestWindowFeature(Window.FEATURE_NO_TITLE)
        dialog.setCancelable(false)
        val view = LayoutInflater.from(this).inflate(R.layout.dialog_incoming_request, null)
        dialog.setContentView(view)
        dialog.window?.setBackgroundDrawable(ColorDrawable(Color.TRANSPARENT))
        dialog.window?.setLayout(
            (resources.displayMetrics.widthPixels * 0.90).toInt(),
            ViewGroup.LayoutParams.WRAP_CONTENT
        )

        val tvSender = view.findViewById<TextView>(R.id.tvIncomingSenderName)
        val tvSub = view.findViewById<TextView>(R.id.tvIncomingSubHeader)
        val tvSummary = view.findViewById<TextView>(R.id.tvIncomingSummary)
        val tvFiles = view.findViewById<TextView>(R.id.tvIncomingFileList)
        val btnDecline = view.findViewById<TextView>(R.id.btnDeclineIncoming)
        val btnAccept = view.findViewById<TextView>(R.id.btnAcceptIncoming)

        tvSender.text = req.senderAlias
        tvSub.text = "Size dosya göndermek istiyor (${req.transportLabel})"
        tvSummary.text = "Dosya Sayısı: ${req.fileCount} • Toplam: ${StorageHelper.formatFileSize(req.totalBytes)}"
        tvFiles.text = req.filePreviewLines

        btnDecline.setOnClickListener {
            dialog.dismiss()
            activeConsentDialog = null
            req.onDecision(false)
            Toast.makeText(this, "Gelen dosya isteği reddedildi.", Toast.LENGTH_SHORT).show()
        }

        btnAccept.setOnClickListener {
            dialog.dismiss()
            activeConsentDialog = null
            binding.cardActiveTransfer.visibility = View.VISIBLE
            binding.btnPauseResumeTransfer.visibility = View.VISIBLE
            binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
            req.onDecision(true)
        }

        activeConsentDialog = dialog
        dialog.show()
    }

    private fun handleIncomingShareIntent(intent: Intent?) {
        val sharedItems = StorageHelper.extractTransferItemsFromIntent(this, intent)
        if (sharedItems.isNotEmpty()) {
            selectedTransferItems.addAll(sharedItems)
            refreshSelectionUi()
            switchTab(1)
            Toast.makeText(
                this,
                "${sharedItems.size} dosya önbelleğe kopyalanmadan (Zero-Cache) doğrudan akışa hazır!",
                Toast.LENGTH_SHORT
            ).show()
        }
    }

    private fun refreshSelectionUi() {
        if (::transferEngine.isInitialized) {
            transferEngine.setItemsForWebLinkShare(selectedTransferItems.toList())
        }
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
            if (combined.none { it.id == p2p.id }) {
                combined.add(p2p)
            }
        }
        combined.sortWith(compareBy<PeerDevice> { it.transport.priority }.thenBy { it.alias })

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

        if (!peer.ipAddress.isNullOrBlank()) {
            transferEngine.sendItemsToPeers(
                items = selectedTransferItems.toList(),
                targetHostIps = listOf(peer.ipAddress to peer.port),
                transportLabel = peer.transport.label
            )
            return
        }

        if (!peer.macAddress.isNullOrBlank()) {
            pendingPeerForDirectTransfer = peer
            binding.cardActiveTransfer.visibility = View.VISIBLE
            binding.tvTransferTitle.text = "5 GHz Doğrudan Köprü Kuruluyor..."
            binding.tvTransferFileName.text = peer.alias
            binding.tvTransferEta.text = "Karşı ekranda çıkan Wi-Fi Direct davetini onaylayın..."
            wifiDirectManager.connectWithCascadingBandFallback(peer.macAddress) { success, msg ->
                runOnUiThread {
                    if (!success) {
                        binding.tvTransferEta.text = msg
                        Toast.makeText(this, msg, Toast.LENGTH_LONG).show()
                    }
                }
            }
        }
    }

    private fun renderTransferProgress(progress: TransferProgress) {
        binding.cardActiveTransfer.visibility = View.VISIBLE
        binding.tvTransferTitle.text = "Zero-Copy (${progress.transportLabel})"
        binding.tvTransferFileName.text = progress.fileName
        binding.progressBarTransfer.progress = progress.progressPercent

        val speedFormat = DecimalFormat("0.0")
        binding.tvTransferSpeedMbps.text = "${speedFormat.format(progress.speedMbps)} MB/s"

        val transferredStr = StorageHelper.formatFileSize(progress.bytesTransferred)
        val totalStr = StorageHelper.formatFileSize(progress.totalBytes)
        binding.tvTransferBytesAndPercent.text = "%${progress.progressPercent} ($transferredStr / $totalStr)"

        if (!progress.savedFilePath.isNullOrEmpty()) {
            lastSavedOrPartialFilePath = progress.savedFilePath
        }

        when (progress.status) {
            TransferStatus.CONNECTING -> {
                binding.tvTransferEta.text = progress.message ?: "Bağlanıyor..."
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.WAITING_APPROVAL -> {
                binding.tvTransferTitle.text = "Karşı Cihazın Onayı Bekleniyor..."
                binding.tvTransferEta.text = progress.message ?: "Alıcı cihazda 'Kabul Et' onayı bekleniyor..."
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.TRANSFERRING -> {
                binding.tvTransferEta.text = "Kalan Süre: ${formatEta(progress.etaSeconds)}"
                binding.btnPauseResumeTransfer.visibility = View.VISIBLE
                binding.btnCancelTransferKeepPartial.visibility = View.VISIBLE
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.PAUSED -> {
                binding.tvTransferEta.text = "Duraklatıldı (Kısmi veri koruma altında)"
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                }
            }
            TransferStatus.PARTIAL_PRESERVED -> {
                binding.tvTransferTitle.text = "Kısmi Dosya Korundu (Mühürlendi)"
                binding.tvTransferEta.text = progress.message ?: "Kısmi dosya korundu ve anında açılabilir!"
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.GONE
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                    binding.btnOpenPartialFile.text = "Kısmi Dosyayı Aç / Oynat"
                }
            }
            TransferStatus.COMPLETED -> {
                binding.tvTransferTitle.text = "Aktarım Tamamlandı"
                binding.tvTransferEta.text = progress.message ?: "Aktarım Tamamlandı"
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.GONE
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                    binding.btnOpenPartialFile.text = "Dosyayı Aç / Oynat"
                }
                if (currentSendMode == SendMode.SINGLE_RECIPIENT) {
                    selectedTransferItems.clear()
                    refreshSelectionUi()
                }
            }
            TransferStatus.REJECTED -> {
                binding.tvTransferTitle.text = "İstek Reddedildi"
                binding.tvTransferEta.text = progress.message ?: "Karşı cihaz dosya isteğini reddetti."
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.GONE
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.ERROR -> {
                binding.tvTransferTitle.text = "Bağlantı Kurulamadı"
                binding.tvTransferEta.text = progress.message ?: "Bağlantı sonlandı."
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.btnCancelTransferKeepPartial.visibility = View.GONE
            }
            TransferStatus.IDLE -> {
                binding.cardActiveTransfer.visibility = View.GONE
            }
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
            val msg = buildString {
                appendLine("• Tek alıcı: Dosyalar tek bir hedef cihaza gönderilir ve aktarım sonunda seçim temizlenir.")
                appendLine()
                appendLine("• Birden fazla alıcı: Seçiminiz korunur ve birden fazla cihaza art arda gönderilir.")
                appendLine()
                append("• Link üzerinden paylaşın: Uygulama yüklü olmayan cihazlar tarayıcıdan (HTTP Port 53319) doğrudan indirebilir.")
            }
            AlertDialog.Builder(this)
                .setTitle("Gönderim Modları")
                .setMessage(msg)
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
        etInput.hint = "Hedef IP Adresi (Örn: 192.168.49.1 veya 192.168.43.1)"
        view.findViewById<TextView>(R.id.btnCancelTextDialog).setOnClickListener { dialog.dismiss() }
        view.findViewById<TextView>(R.id.btnConfirmTextDialog).setOnClickListener {
            val ip = etInput.text.toString().trim()
            if (ip.isNotEmpty()) {
                lanDiscoveryManager.sendUnicastAnnounce(ip)
                if (selectedTransferItems.isNotEmpty()) {
                    transferEngine.sendItemsToPeers(
                        items = selectedTransferItems.toList(),
                        targetHostIps = listOf(ip to 53317),
                        transportLabel = "Doğrudan IP ($ip)"
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
            .setItems(names) { _: DialogInterface, which: Int ->
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
            .setTitle("Alınan Dosyalar (Oynatmak için dokunun)")
            .setItems(labels) { _: DialogInterface, idx: Int ->
                val rec = records[idx]
                openFileInExternalPlayer(File(rec.filePath))
            }
            .setNeutralButton("Geçmişi Temizle") { _: DialogInterface, _: Int ->
                transferEngine.clearHistory()
                lastSavedOrPartialFilePath = null
                binding.cardActiveTransfer.visibility = View.GONE
                Toast.makeText(this, "Aktarım geçmişi temizlendi.", Toast.LENGTH_SHORT).show()
            }
            .setPositiveButton("Kapat", null)
            .show()
    }

    private fun showNetworkAndDeviceInfoDialog() {
        val ips = lanDiscoveryManager.getLocalIpAddresses()
        val ipText = if (ips.isEmpty()) "Wi-Fi / Hotspot Bekleniyor" else ips.joinToString(", ")
        val hwBands = wifiDirectManager.getHardwareBandCapabilitySummary()
        val msg = buildString {
            appendLine("• Cihaz Adı: $deviceAlias")
            appendLine("• Desteklenen Bantlar: $hwBands")
            appendLine("• Aktif Wi-Fi / Köprü IP: $ipText")
            appendLine("• Zero-Copy TCP Portu: 53317 (16 MB Tampon / 4 MB Blok)")
            append("• Hedef Klasör: /storage/emulated/0/Download")
        }

        AlertDialog.Builder(this)
            .setTitle("Donanım & Ağ Durumu")
            .setMessage(msg)
            .setPositiveButton("Tamam", null)
            .show()
    }

    private fun showWebLinkInfoDialog() {
        val ips = lanDiscoveryManager.getLocalIpAddresses()
        val primaryIp = ips.firstOrNull() ?: "192.168.49.1"
        val url = "http://$primaryIp:53319"
        val msg = buildString {
            appendLine("Karşı cihazda uygulama yüklü olmasa bile aynı Wi-Fi veya Doğrudan Köprü ağına bağlanıp tarayıcıdan şu adrese girerek dosyaları doğrudan indirebilirsiniz:")
            appendLine()
            append(url)
        }

        AlertDialog.Builder(this)
            .setTitle("Link Üzerinden Al / Paylaş")
            .setMessage(msg)
            .setPositiveButton("Tamam", null)
            .show()
    }

    private fun showTroubleshootDialog() {
        val hwBands = wifiDirectManager.getHardwareBandCapabilitySummary()
        val ips = lanDiscoveryManager.getLocalIpAddresses().joinToString(", ").ifEmpty { "Yok" }
        val msg = buildString {
            appendLine("• Donanım Bant Desteği: $hwBands")
            appendLine("• Aktif IP Adresleri: $ips")
            appendLine()
            appendLine("Modemsiz (Çevrimdışı) 5 GHz Maksimum Hız İçin 2 Yöntem:")
            appendLine("1) Alım sekmesindeki '5 GHz Doğrudan Köprü Kur' butonuna basın; ekranda çıkan DIRECT-... 5 GHz ağına diğer telefonun Wi-Fi ayarlarından bağlanın.")
            append("2) Veya telefonlardan birinde 5 GHz Mobil Erişim Noktası (Hotspot) açıp diğer telefonu ona bağlayın; Zeph internet harcamadan doğrudan 5 GHz donanım hızında aktarım yapar.")
        }
        AlertDialog.Builder(this)
            .setTitle("Maksimum Hız & Sorun Giderme")
            .setMessage(msg)
            .setPositiveButton("5 GHz Doğrudan Köprü Kur") { _: DialogInterface, _: Int ->
                binding.btnCreate5GhzBridge.performClick()
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
            val uri: Uri = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                FileProvider.getUriForFile(
                    this,
                    "${applicationContext.packageName}.fileprovider",
                    file
                )
            } else {
                Uri.fromFile(file)
            }

            val intent = Intent(Intent.ACTION_VIEW).apply {
                setDataAndType(uri, mime)
                addFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_GRANT_READ_URI_PERMISSION)
            }
            startActivity(Intent.createChooser(intent, "Dosyayı Oynat / Aç (${file.name})"))
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
        activeConsentDialog?.dismiss()
        skeletonAnimationJob?.cancel()
        wifiDirectManager.stop()
        lanDiscoveryManager.stop()
        transferEngine.stop()
        if (!binding.switchKeepServiceNotification.isChecked) {
            setHardwareServiceRunning(false)
        }
        super.onDestroy()
    }
}
