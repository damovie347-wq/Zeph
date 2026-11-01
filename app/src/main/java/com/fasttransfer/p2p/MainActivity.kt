package com.fasttransfer.p2p

import android.app.Dialog
import android.content.Context
import android.content.DialogInterface
import android.content.Intent
import android.content.res.ColorStateList
import android.graphics.Color
import android.graphics.drawable.ColorDrawable
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.os.StrictMode
import android.text.Editable
import android.text.TextWatcher
import android.transition.AutoTransition
import android.transition.TransitionManager
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
import androidx.activity.OnBackPressedCallback
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
import com.fasttransfer.p2p.model.AppThemeMode
import com.fasttransfer.p2p.model.ConnectionTransport
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
    private var currentThemeMode: AppThemeMode = AppThemeMode.DARK_SLATE
    private var currentTabIndex: Int = 0
    private var deviceAlias: String = "Android"
    private var lastSavedOrPartialFilePath: String? = null
    private var pendingPeerForDirectTransfer: PeerDevice? = null
    private var p2pWaitClientJob: Job? = null
    private var currentIncomingRequest: IncomingTransferRequest? = null
    private var isAdvancedStatsExpanded: Boolean = false
    private var isTransferCurrentlyFinishedOrStopped: Boolean = false

    private var skeletonAnimationJob: Job? = null

    private val permissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) {
        wifiDirectManager.syncStateAndDiscover()
        lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
    }

    private val pickFilesLauncher = registerForActivityResult(
        ActivityResultContracts.OpenMultipleDocuments()
    ) { uris: List<Uri> ->
        if (uris.isNotEmpty()) {
            val newItems = mutableListOf<TransferItem>()
            for (uri in uris) {
                try {
                    contentResolver.takePersistableUriPermission(
                        uri,
                        Intent.FLAG_GRANT_READ_URI_PERMISSION
                    )
                } catch (_: Throwable) {
                }
                StorageHelper.buildTransferItemFromUri(this, uri)?.let { item ->
                    newItems.add(item)
                }
            }
            addUniqueTransferItems(newItems)
        }
    }

    private val pickMediaLauncher = registerForActivityResult(
        ActivityResultContracts.GetMultipleContents()
    ) { uris: List<Uri> ->
        if (uris.isNotEmpty()) {
            val newItems = mutableListOf<TransferItem>()
            for (uri in uris) {
                StorageHelper.buildTransferItemFromUri(this, uri)?.let { item ->
                    newItems.add(item)
                }
            }
            addUniqueTransferItems(newItems)
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
                        addUniqueTransferItems(folderItems)
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
        setupIncomingAndSessionOverlays()
        setupSettingsTabActions()
        setupBackNavigationHandler()
        applyThemeMode(currentThemeMode)
        setupEngines()

        requestNecessaryPermissionsIfNeeded()
        handleIncomingShareIntent(intent)
    }

    override fun onResume() {
        super.onResume()
        if (::wifiDirectManager.isInitialized && ::lanDiscoveryManager.isInitialized) {
            wifiDirectManager.syncStateAndDiscover()
            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        handleIncomingShareIntent(intent)
    }

    private fun addUniqueTransferItems(incoming: List<TransferItem>) {
        var addedCount = 0
        var skippedDuplicates = 0
        for (candidate in incoming) {
            val alreadyExists = selectedTransferItems.any { existing ->
                (existing.name.equals(candidate.name, ignoreCase = true) && existing.sizeBytes == candidate.sizeBytes) ||
                    (existing.uri != null && existing.uri == candidate.uri)
            }
            if (alreadyExists) {
                skippedDuplicates++
            } else {
                selectedTransferItems.add(candidate)
                addedCount++
            }
        }
        refreshSelectionUi()
        if (skippedDuplicates > 0 && addedCount == 0) {
            Toast.makeText(this, "Bu dosya zaten seçiminizde mevcut.", Toast.LENGTH_SHORT).show()
        }
    }

    private fun loadPreferences() {
        val prefs = getSharedPreferences("fast_transfer_prefs", Context.MODE_PRIVATE)
        val realModelName = CompatibilityHelper.resolveUniqueDeviceName(this)
        val savedAlias = prefs.getString("device_alias", null)

        deviceAlias = if (CompatibilityHelper.isPlaceholderDeviceName(savedAlias)) {
            realModelName
        } else {
            savedAlias!!
        }

        val savedFavs = prefs.getStringSet("favorite_ips", emptySet()) ?: emptySet()
        val autoAccept = prefs.getBoolean("auto_accept_incoming", false)
        val keepNotif = prefs.getBoolean("keep_service_notif", false)
        val themePref = prefs.getInt("app_theme_mode", AppThemeMode.DARK_SLATE.prefValue)
        currentThemeMode = AppThemeMode.fromPref(themePref)

        favoriteIps.clear()
        for (ip in savedFavs) {
            if (ip != "192.168.49.1") {
                favoriteIps.add(ip)
            }
        }

        binding.tvDeviceAliasReceive.text = deviceAlias
        binding.etSettingsDeviceAlias.setText(deviceAlias)
        binding.switchAutoAccept.isChecked = autoAccept
        binding.switchKeepServiceNotification.isChecked = keepNotif
        savePreferences()
    }

    private fun savePreferences() {
        val prefs = getSharedPreferences("fast_transfer_prefs", Context.MODE_PRIVATE)
        prefs.edit()
            .putString("device_alias", deviceAlias)
            .putStringSet("favorite_ips", favoriteIps)
            .putBoolean("auto_accept_incoming", binding.switchAutoAccept.isChecked)
            .putBoolean("keep_service_notif", binding.switchKeepServiceNotification.isChecked)
            .putInt("app_theme_mode", currentThemeMode.prefValue)
            .apply()
    }

    private fun applyThemeMode(mode: AppThemeMode) {
        currentThemeMode = mode
        savePreferences()

        val bgMain: Int
        val bgNav: Int
        val textPrimary: Int
        val textSecondary: Int
        val radarColor: Int

        when (mode) {
            AppThemeMode.AMOLED_BLACK -> {
                bgMain = Color.parseColor("#000000")
                bgNav = Color.parseColor("#080C0E")
                textPrimary = Color.parseColor("#FFFFFF")
                textSecondary = Color.parseColor("#90A4AE")
                radarColor = Color.parseColor("#6CD4FF")
            }
            AppThemeMode.DARK_SLATE -> {
                bgMain = Color.parseColor("#101818")
                bgNav = Color.parseColor("#141E1E")
                textPrimary = Color.parseColor("#ECF4F6")
                textSecondary = Color.parseColor("#93A8AC")
                radarColor = Color.parseColor("#6CD4FF")
            }
            AppThemeMode.LIGHT_CLEAN -> {
                bgMain = Color.parseColor("#F2F7F9")
                bgNav = Color.parseColor("#E1ECEF")
                textPrimary = Color.parseColor("#0D1F26")
                textSecondary = Color.parseColor("#4A6572")
                radarColor = Color.parseColor("#0088B8")
            }
        }

        binding.rootFrameContainer.setBackgroundColor(bgMain)
        binding.bottomNavBar.setBackgroundColor(bgNav)
        binding.radarPulseReceive.setThemeColor(radarColor)
        binding.tvDeviceAliasReceive.setTextColor(textPrimary)
        binding.tvSettingsMainHeader.setTextColor(textPrimary)
        binding.tvDynamicTip.setTextColor(textSecondary)
        binding.btnReceiveHistory.imageTintList = ColorStateList.valueOf(textPrimary)
        binding.btnReceiveInfo.imageTintList = ColorStateList.valueOf(textPrimary)

        window.statusBarColor = bgMain
        window.navigationBarColor = bgNav

        val darkText = Color.parseColor("#002A3A")
        val whiteText = Color.parseColor("#ECF4F6")

        binding.btnThemeAmoled.setBackgroundResource(
            if (mode == AppThemeMode.AMOLED_BLACK) R.drawable.bg_button_cyan_pill else R.drawable.bg_button_outline_pill
        )
        binding.btnThemeAmoled.setTextColor(if (mode == AppThemeMode.AMOLED_BLACK) darkText else whiteText)

        binding.btnThemeDark.setBackgroundResource(
            if (mode == AppThemeMode.DARK_SLATE) R.drawable.bg_button_cyan_pill else R.drawable.bg_button_outline_pill
        )
        binding.btnThemeDark.setTextColor(if (mode == AppThemeMode.DARK_SLATE) darkText else whiteText)

        binding.btnThemeLight.setBackgroundResource(
            if (mode == AppThemeMode.LIGHT_CLEAN) R.drawable.bg_button_cyan_pill else R.drawable.bg_button_outline_pill
        )
        binding.btnThemeLight.setTextColor(if (mode == AppThemeMode.LIGHT_CLEAN) darkText else whiteText)

        switchTab(currentTabIndex)
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
                stopService(Intent(this, TransferForegroundService::class.java))
            } catch (_: Throwable) {
            }
        }
    }

    private fun setupSplashAndRadarViews() {
        binding.radarPulseSplash.setThemeColor(Color.parseColor("#00A884"))
        binding.radarPulseReceive.setThemeColor(Color.parseColor("#6CD4FF"))

        lifecycleScope.launch {
            delay(750L)
            binding.splashOverlay.animate()
                .alpha(0f)
                .setDuration(240L)
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
        val previousTab = currentTabIndex
        currentTabIndex = index
        binding.pageReceive.visibility = if (index == 0) View.VISIBLE else View.GONE
        binding.pageSend.visibility = if (index == 1) View.VISIBLE else View.GONE
        binding.pageSettings.visibility = if (index == 2) View.VISIBLE else View.GONE

        val activeColor = if (currentThemeMode == AppThemeMode.LIGHT_CLEAN) {
            Color.parseColor("#FFFFFF")
        } else {
            ContextCompat.getColor(this, R.color.text_primary_white)
        }
        val activeLabelColor = if (currentThemeMode == AppThemeMode.LIGHT_CLEAN) {
            Color.parseColor("#0D1F26")
        } else {
            ContextCompat.getColor(this, R.color.text_primary_white)
        }
        val muted = if (currentThemeMode == AppThemeMode.LIGHT_CLEAN) {
            Color.parseColor("#5C7682")
        } else {
            ContextCompat.getColor(this, R.color.text_secondary_muted)
        }

        binding.pillReceive.setBackgroundResource(if (index == 0) R.drawable.bg_nav_pill else 0)
        binding.ivNavReceive.setColorFilter(if (index == 0) activeColor else muted)
        binding.tvNavReceive.setTextColor(if (index == 0) activeLabelColor else muted)

        binding.pillSend.setBackgroundResource(if (index == 1) R.drawable.bg_nav_pill else 0)
        binding.ivNavSend.setColorFilter(if (index == 1) activeColor else muted)
        binding.tvNavSend.setTextColor(if (index == 1) activeLabelColor else muted)

        binding.pillSettings.setBackgroundResource(if (index == 2) R.drawable.bg_nav_pill else 0)
        binding.ivNavSettings.setColorFilter(if (index == 2) activeColor else muted)
        binding.tvNavSettings.setTextColor(if (index == 2) activeLabelColor else muted)

        if (::wifiDirectManager.isInitialized && ::lanDiscoveryManager.isInitialized && previousTab != index) {
            wifiDirectManager.syncStateAndDiscover()
            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
        }
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

    private fun setupIncomingAndSessionOverlays() {
        isAdvancedStatsExpanded = false
        binding.layoutAdvancedStats.visibility = View.GONE
        binding.tvToggleAdvancedLabel.text = "Gelişmiş"

        binding.btnOverlayVerify.setOnClickListener {
            val req = currentIncomingRequest ?: return@setOnClickListener
            AlertDialog.Builder(this)
                .setTitle("Bağlantı Doğrulaması")
                .setMessage("Gönderici: ${req.senderAlias} (${req.senderModel})\nIP: ${req.senderIp}\nKanal: ${req.transportLabel}")
                .setPositiveButton("Tamam", null)
                .show()
        }

        binding.btnOverlayOptions.setOnClickListener {
            binding.overlayIncomingOptions.alpha = 0f
            binding.overlayIncomingOptions.translationX = 80f
            binding.overlayIncomingOptions.visibility = View.VISIBLE
            binding.overlayIncomingOptions.animate()
                .alpha(1f)
                .translationX(0f)
                .setDuration(220L)
                .start()
        }

        binding.btnCloseIncomingOptions.setOnClickListener {
            binding.overlayIncomingOptions.animate()
                .alpha(0f)
                .translationX(80f)
                .setDuration(180L)
                .withEndAction {
                    binding.overlayIncomingOptions.visibility = View.GONE
                }
                .start()
        }

        binding.btnOverlayDecline.setOnClickListener {
            val req = currentIncomingRequest
            currentIncomingRequest = null
            hideOverlaySmoothly(binding.overlayIncomingOptions)
            hideOverlaySmoothly(binding.overlayIncomingRequest)
            req?.onDecision?.invoke(false)
        }

        binding.btnOverlayAccept.setOnClickListener {
            val req = currentIncomingRequest
            currentIncomingRequest = null
            binding.overlayIncomingOptions.visibility = View.GONE
            hideOverlaySmoothly(binding.overlayIncomingRequest)
            showTransferSessionOverlay(
                isSending = false,
                initialFileName = req?.firstFileName ?: "Dosya",
                initialFileSize = req?.firstFileSize ?: 0L
            )
            req?.onDecision?.invoke(true)
        }

        binding.btnToggleAdvancedStats.setOnClickListener {
            val transition = AutoTransition().apply {
                duration = 220L
            }
            TransitionManager.beginDelayedTransition(binding.overlayTransferSession, transition)
            isAdvancedStatsExpanded = !isAdvancedStatsExpanded
            binding.layoutAdvancedStats.visibility = if (isAdvancedStatsExpanded) View.VISIBLE else View.GONE
            binding.tvToggleAdvancedLabel.text = if (isAdvancedStatsExpanded) "Gizle" else "Gelişmiş"
        }

        binding.btnPauseResumeTransfer.setOnClickListener {
            val paused = transferEngine.togglePauseResume()
            binding.btnPauseResumeTransfer.text = if (paused) "Devam Et" else "Duraklat"
        }

        binding.btnCancelTransferKeepPartial.setOnClickListener {
            if (isTransferCurrentlyFinishedOrStopped) {
                hideOverlaySmoothly(binding.overlayTransferSession)
                return@setOnClickListener
            }
            p2pWaitClientJob?.cancel()
            transferEngine.cancelAndPreservePartialFile()
            isTransferCurrentlyFinishedOrStopped = true
            binding.btnPauseResumeTransfer.visibility = View.GONE
            binding.tvCancelOrDoneLabel.text = "Kapat"
            binding.tvTransferTitle.text = "Kısmi Dosya Korundu"
            if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                binding.btnOpenPartialFile.visibility = View.VISIBLE
            }
            Toast.makeText(
                this,
                "Aktarım durduruldu! O ana kadar inen kısmi dosya korundu.",
                Toast.LENGTH_SHORT
            ).show()
        }

        binding.btnDismissTransferCard.setOnClickListener {
            p2pWaitClientJob?.cancel()
            transferEngine.cancelAndPreservePartialFile()
            isTransferCurrentlyFinishedOrStopped = true
            hideOverlaySmoothly(binding.overlayTransferSession)
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

    private fun showTransferSessionOverlay(isSending: Boolean, initialFileName: String, initialFileSize: Long) {
        isTransferCurrentlyFinishedOrStopped = false
        binding.tvSessionMainTitle.text = if (isSending) "Dosyalar gönderiliyor" else "Dosyalar alınıyor"
        binding.tvSessionSubPath.text = if (isSending) {
            "Zero-Copy 32 MB Çift Çekirdekli DMA Akışı (5 GHz / 6 GHz)"
        } else {
            "Hedef klasör: /storage/emulated/0/Download"
        }
        binding.tvSessionItemTitle.text = "$initialFileName (${StorageHelper.formatFileSize(initialFileSize)})"
        binding.progressSessionItem.progress = 0
        binding.progressBarTransfer.progress = 0
        binding.tvTransferTitle.text = "Toplam ilerleme (--:--)"
        binding.tvTransferElapsedTime.text = "Geçen Süre: 0.0 sn"
        binding.btnPauseResumeTransfer.text = "Duraklat"
        binding.btnPauseResumeTransfer.visibility = View.VISIBLE
        binding.tvCancelOrDoneLabel.text = "İptal et"
        binding.btnOpenPartialFile.visibility = View.GONE

        if (binding.overlayTransferSession.visibility != View.VISIBLE) {
            binding.overlayTransferSession.alpha = 0f
            binding.overlayTransferSession.translationY = 48f
            binding.overlayTransferSession.visibility = View.VISIBLE
            binding.overlayTransferSession.animate()
                .alpha(1f)
                .translationY(0f)
                .setDuration(240L)
                .start()
        }
    }

    private fun hideOverlaySmoothly(view: View) {
        if (view.visibility != View.VISIBLE) return
        view.animate()
            .alpha(0f)
            .translationY(36f)
            .setDuration(180L)
            .withEndAction {
                view.visibility = View.GONE
                view.translationY = 0f
            }
            .start()
    }

    private fun setupBackNavigationHandler() {
        onBackPressedDispatcher.addCallback(this, object : OnBackPressedCallback(true) {
            override fun handleOnBackPressed() {
                when {
                    binding.overlayIncomingOptions.visibility == View.VISIBLE -> {
                        binding.btnCloseIncomingOptions.performClick()
                    }
                    binding.overlayIncomingRequest.visibility == View.VISIBLE -> {
                        binding.btnOverlayDecline.performClick()
                    }
                    binding.overlayTransferSession.visibility == View.VISIBLE -> {
                        if (isTransferCurrentlyFinishedOrStopped) {
                            hideOverlaySmoothly(binding.overlayTransferSession)
                        } else {
                            binding.btnCancelTransferKeepPartial.performClick()
                        }
                    }
                    else -> {
                        isEnabled = false
                        onBackPressedDispatcher.onBackPressed()
                        isEnabled = true
                    }
                }
            }
        })
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
            wifiDirectManager.syncStateAndDiscover()
            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
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
    }

    private fun setupSettingsTabActions() {
        binding.btnThemeAmoled.setOnClickListener {
            applyThemeMode(AppThemeMode.AMOLED_BLACK)
        }
        binding.btnThemeDark.setOnClickListener {
            applyThemeMode(AppThemeMode.DARK_SLATE)
        }
        binding.btnThemeLight.setOnClickListener {
            applyThemeMode(AppThemeMode.LIGHT_CLEAN)
        }

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
            wifiDirectManager.preferHighBand = isChecked
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
        }

        binding.btnDisconnectBridgeNow.setOnClickListener {
            wifiDirectManager.cleanupStaleGroups()
        }

        binding.btnStopServiceNotification.setOnClickListener {
            binding.switchKeepServiceNotification.isChecked = false
            savePreferences()
            setHardwareServiceRunning(false)
        }

        binding.btnClearHistorySettings.setOnClickListener {
            transferEngine.clearHistory()
            lastSavedOrPartialFilePath = null
            hideOverlaySmoothly(binding.overlayTransferSession)
            Toast.makeText(this, "Aktarım geçmişi temizlendi.", Toast.LENGTH_SHORT).show()
        }
    }

    private fun setupEngines() {
        if (binding.switchKeepServiceNotification.isChecked) {
            setHardwareServiceRunning(true)
        } else {
            setHardwareServiceRunning(false)
        }

        wifiDirectManager = CascadingWifiDirectManager(
            context = this,
            getLocalAlias = { deviceAlias },
            onPeersUpdated = { peers ->
                runOnUiThread {
                    p2pPeers.clear()
                    p2pPeers.addAll(peers)
                    mergeAndRenderDiscoveredPeers()
                }
            },
            onDirectConnectionReady = { groupOwnerIp, isGroupOwner, bandLabel ->
                runOnUiThread {
                    lifecycleScope.launch(Dispatchers.IO) {
                        repeat(6) {
                            if (!isGroupOwner) {
                                lanDiscoveryManager.sendUnicastAnnounce(groupOwnerIp)
                            }
                            lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                            delay(200L)
                        }
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
                            p2pWaitClientJob?.cancel()
                            p2pWaitClientJob = lifecycleScope.launch(Dispatchers.IO) {
                                var targetClientIp: String? = null
                                for (attempt in 0 until 45) {
                                    val fromLan = lanPeers.firstOrNull {
                                        it.ipAddress?.startsWith("192.168.49.") == true && it.ipAddress != "192.168.49.1"
                                    }?.ipAddress
                                    val fromArp = wifiDirectManager.findConnectedP2pClientIpFromArp()
                                    targetClientIp = fromLan ?: fromArp
                                    if (!targetClientIp.isNullOrBlank()) break
                                    lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                                    delay(220L)
                                }
                                withContext(Dispatchers.Main) {
                                    if (!targetClientIp.isNullOrBlank()) {
                                        transferEngine.sendItemsToPeers(
                                            items = selectedTransferItems.toList(),
                                            targetHostIps = listOf(targetClientIp to 53317),
                                            transportLabel = bandLabel
                                        )
                                    } else {
                                        binding.tvTransferTitle.text = "Köprü Hazır (192.168.49.1)"
                                        binding.tvTransferEta.text = "Bağlantı kuruldu, şimdi cihaza tekrar dokunarak gönderimi başlatın."
                                    }
                                }
                            }
                        }
                    }
                }
            },
            onDirectDisconnected = {
                if (::lanDiscoveryManager.isInitialized) {
                    lanDiscoveryManager.removePeersByPrefix("192.168.49.")
                }
            },
            onStatusLog = { _ ->
            }
        )

        lanDiscoveryManager = LanDiscoveryManager(
            context = this,
            getLocalAlias = { deviceAlias },
            getLocalP2pGroup = { wifiDirectManager.getActiveGroupCredentials() },
            onLanPeersChanged = { peers ->
                runOnUiThread {
                    lanPeers.clear()
                    lanPeers.addAll(peers)
                    mergeAndRenderDiscoveredPeers()
                }
            }
        )

        transferEngine = ZeroCopyTransferEngine(
            context = this,
            getLocalAlias = { deviceAlias },
            getLocalDeviceModel = { lanDiscoveryManager.getLocalDeviceModelName() },
            onPrepareP2pGroup = {
                wifiDirectManager.createAutonomousHighSpeedGroupBlocking(3600L)
            },
            onJoinP2pGroup = { creds ->
                wifiDirectManager.joinAutonomousGroupSilentlyBlocking(creds, 3400L)
            },
            isHighBandUpgradeEnabled = {
                wifiDirectManager.preferHighBand && !wifiDirectManager.shouldSkipP2pBecauseHotspotOrWifiOff()
            },
            onIncomingConsentRequest = { req ->
                runOnUiThread {
                    showIncomingConsentOverlay(req)
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
                    wifiDirectManager.setTransferInProgress(isActiveTransfer)
                    if (binding.switchKeepServiceNotification.isChecked) {
                        setHardwareServiceRunning(isActiveTransfer)
                    } else {
                        setHardwareServiceRunning(false)
                    }
                }
            }
        )
        transferEngine.autoAcceptIncoming = binding.switchAutoAccept.isChecked
        transferEngine.startServer()

        wifiDirectManager.start()
        lanDiscoveryManager.start()

        for (favIp in favoriteIps) {
            lanDiscoveryManager.addManualFavoritePeer(favIp)
        }
    }

    private fun showIncomingConsentOverlay(req: IncomingTransferRequest) {
        currentIncomingRequest = req
        binding.tvOverlaySenderAlias.text = req.senderAlias
        binding.tvOverlaySenderModelBadge.text = req.senderModel
        binding.tvOverlayWantToSend.text = if (req.fileCount > 1) {
            "sana ${req.fileCount} dosya göndermek istiyor"
        } else {
            "sana bir dosya göndermek istiyor"
        }

        binding.tvOptionsFileName.text = req.firstFileName
        binding.tvOptionsFileSize.text = "Değiştirilmemiş - ${StorageHelper.formatFileSize(req.firstFileSize)}"

        binding.overlayIncomingOptions.visibility = View.GONE
        binding.overlayIncomingRequest.alpha = 0f
        binding.overlayIncomingRequest.translationY = 48f
        binding.overlayIncomingRequest.visibility = View.VISIBLE
        binding.overlayIncomingRequest.animate()
            .alpha(1f)
            .translationY(0f)
            .setDuration(240L)
            .start()
    }

    private fun handleIncomingShareIntent(intent: Intent?) {
        val sharedItems = StorageHelper.extractTransferItemsFromIntent(this, intent)
        if (sharedItems.isNotEmpty()) {
            addUniqueTransferItems(sharedItems)
            switchTab(1)
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

        val sortedLan = lanPeers.sortedBy { if (it.ipAddress?.startsWith("192.168.49.") == true) 0 else 1 }
        for (lan in sortedLan) {
            val existingIdx = combined.indexOfFirst { it.alias.equals(lan.alias, ignoreCase = true) }
            if (existingIdx == -1) {
                combined.add(lan)
            } else {
                val existing = combined[existingIdx]
                if (existing.ipAddress?.startsWith("192.168.49.") != true && lan.ipAddress?.startsWith("192.168.49.") == true) {
                    combined[existingIdx] = lan
                }
            }
        }

        for (p2p in p2pPeers) {
            val matchIdx = combined.indexOfFirst {
                it.id == p2p.id || it.alias.equals(p2p.alias, ignoreCase = true)
            }
            if (matchIdx == -1) {
                combined.add(p2p)
            } else {
                val existing = combined[matchIdx]
                combined[matchIdx] = existing.copy(
                    macAddress = existing.macAddress ?: p2p.macAddress,
                    p2pSsid = existing.p2pSsid ?: p2p.p2pSsid,
                    p2pPassphrase = existing.p2pPassphrase ?: p2p.p2pPassphrase,
                    p2pFreqMhz = if (existing.p2pFreqMhz > 0) existing.p2pFreqMhz else p2p.p2pFreqMhz,
                    transport = ConnectionTransport.WIFI_DIRECT_AUTO
                )
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

        val first = selectedTransferItems.first()
        showTransferSessionOverlay(
            isSending = true,
            initialFileName = first.name,
            initialFileSize = first.sizeBytes
        )

        if (currentSendMode == SendMode.MULTI_RECIPIENT) {
            val allTargets = lanPeers.mapNotNull { p -> p.ipAddress?.let { it to p.port } }.distinct()
            if (allTargets.isNotEmpty()) {
                transferEngine.sendItemsToPeers(
                    items = selectedTransferItems.toList(),
                    targetHostIps = allTargets,
                    transportLabel = "Çoklu Alıcı (${allTargets.size} Cihaz)"
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
            binding.tvTransferTitle.text = "5 GHz / 6 GHz Doğrudan Köprü Kuruluyor..."
            binding.tvTransferEta.text = "Karşı cihazda çıkan Wi-Fi Direct davetini onaylayın..."
            wifiDirectManager.connectWithCascadingBandFallback(
                macAddress = peer.macAddress,
                peerSsid = peer.p2pSsid,
                peerPassphrase = peer.p2pPassphrase,
                peerFreqMhz = peer.p2pFreqMhz
            ) { success, msg ->
                runOnUiThread {
                    if (!success) {
                        binding.tvTransferEta.text = msg
                    }
                }
            }
        }
    }

    private fun renderTransferProgress(progress: TransferProgress) {
        if (binding.overlayTransferSession.visibility != View.VISIBLE && progress.status != TransferStatus.IDLE) {
            showTransferSessionOverlay(
                isSending = progress.isSending,
                initialFileName = progress.fileName,
                initialFileSize = progress.totalBytes
            )
        }

        binding.tvSessionMainTitle.text = if (progress.isSending) "Dosyalar gönderiliyor" else "Dosyalar alınıyor"
        binding.tvSessionItemTitle.text = "${progress.fileName} (${StorageHelper.formatFileSize(progress.totalBytes)})"
        binding.progressSessionItem.progress = progress.progressPercent
        binding.progressBarTransfer.progress = progress.progressPercent

        val speedFormat = DecimalFormat("0.0")
        val transferredStr = StorageHelper.formatFileSize(progress.bytesTransferred)
        val totalStr = StorageHelper.formatFileSize(progress.totalBytes)
        val elapsedSecStr = formatElapsedSeconds(progress.elapsedMillis)

        binding.tvAdvancedFilesCount.text = "Dosyalar: ${progress.fileIndex} / ${progress.totalFiles}"
        binding.tvTransferBytesAndPercent.text = "Boyut: $transferredStr / $totalStr"
        binding.tvTransferSpeedMbps.text = "Hız: ${speedFormat.format(progress.speedMbps)} MB/s"
        binding.tvTransferElapsedTime.text = "Geçen Süre: $elapsedSecStr"

        if (!progress.savedFilePath.isNullOrEmpty()) {
            lastSavedOrPartialFilePath = progress.savedFilePath
        }

        when (progress.status) {
            TransferStatus.CONNECTING -> {
                binding.tvTransferTitle.text = "Bağlanıyor..."
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "İptal et"
            }
            TransferStatus.WAITING_APPROVAL -> {
                binding.tvTransferTitle.text = "Karşı Cihazın Onayı Bekleniyor..."
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "İptal et"
            }
            TransferStatus.TRANSFERRING -> {
                isTransferCurrentlyFinishedOrStopped = false
                binding.tvTransferTitle.text = "Toplam ilerleme (${formatClockEta(progress.etaSeconds)})"
                binding.tvTransferEta.text = progress.transportLabel
                binding.btnPauseResumeTransfer.visibility = View.VISIBLE
                binding.tvCancelOrDoneLabel.text = "İptal et"
                binding.btnOpenPartialFile.visibility = View.GONE
            }
            TransferStatus.PAUSED -> {
                binding.tvTransferTitle.text = "Toplam ilerleme (Duraklatıldı)"
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                }
            }
            TransferStatus.PARTIAL_PRESERVED -> {
                isTransferCurrentlyFinishedOrStopped = true
                binding.tvTransferTitle.text = "Kısmi Dosya Korundu (%${progress.progressPercent})"
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "Kapat"
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                    binding.btnOpenPartialFile.text = "Kısmi Dosyayı Aç / Oynat"
                }
            }
            TransferStatus.COMPLETED -> {
                isTransferCurrentlyFinishedOrStopped = true
                binding.progressSessionItem.progress = 100
                binding.progressBarTransfer.progress = 100
                if (!isAdvancedStatsExpanded) {
                    isAdvancedStatsExpanded = true
                    binding.layoutAdvancedStats.visibility = View.VISIBLE
                    binding.tvToggleAdvancedLabel.text = "Gizle"
                }
                binding.tvTransferTitle.text = "Aktarım Tamamlandı (%100)"
                binding.tvTransferSpeedMbps.text = "Ortalama Hız: ${speedFormat.format(progress.speedMbps)} MB/s"
                binding.tvTransferElapsedTime.text = "Toplam Geçen Süre: $elapsedSecStr"
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "Kapat"
                if (!lastSavedOrPartialFilePath.isNullOrEmpty()) {
                    binding.btnOpenPartialFile.visibility = View.VISIBLE
                    binding.btnOpenPartialFile.text = "Dosyayı Aç / Oynat"
                }
                if (progress.isSending && currentSendMode == SendMode.SINGLE_RECIPIENT) {
                    selectedTransferItems.clear()
                    refreshSelectionUi()
                }
            }
            TransferStatus.REJECTED -> {
                isTransferCurrentlyFinishedOrStopped = true
                binding.tvTransferTitle.text = "İstek Reddedildi"
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "Kapat"
            }
            TransferStatus.ERROR -> {
                isTransferCurrentlyFinishedOrStopped = true
                binding.tvTransferTitle.text = "Bağlantı Kurulamadı"
                binding.tvTransferEta.text = progress.message ?: ""
                binding.btnPauseResumeTransfer.visibility = View.GONE
                binding.tvCancelOrDoneLabel.text = "Kapat"
            }
            TransferStatus.IDLE -> {
                hideOverlaySmoothly(binding.overlayTransferSession)
            }
        }
    }

    private fun formatClockEta(seconds: Long): String {
        if (seconds <= 0L) return "0:01"
        val mins = seconds / 60L
        val secs = seconds % 60L
        return String.format("%d:%02d", mins, secs)
    }

    private fun formatElapsedSeconds(elapsedMs: Long): String {
        val sec = elapsedMs / 1000.0
        if (sec < 60.0) {
            return String.format("%.1f sn", sec)
        }
        val m = (sec / 60).toInt()
        val s = (sec % 60).toInt()
        return "${m} dk ${s} sn"
    }

    private fun handleClipboardPasteSelection() {
        val clipItem = StorageHelper.readClipboardAsTransferItem(this)
        if (clipItem != null) {
            addUniqueTransferItems(listOf(clipItem))
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
                addUniqueTransferItems(listOf(item))
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
        val item = TransferItem(
            id = UUID.randomUUID().toString(),
            name = cleanFileName,
            sizeBytes = app.sizeBytes,
            mimeType = "application/vnd.android.package-archive",
            uri = Uri.fromFile(app.apkFile),
            directFile = app.apkFile
        )
        addUniqueTransferItems(listOf(item))
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
                tvStatus.text = favoriteIps.joinToString(10.toChar().toString()) { "• $it (Port 53317)" }
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
        etInput.hint = "Hedef IP Adresi (Örn: 192.168.49.1 veya 192.168.1.92)"
        view.findViewById<TextView>(R.id.btnCancelTextDialog).setOnClickListener { dialog.dismiss() }
        view.findViewById<TextView>(R.id.btnConfirmTextDialog).setOnClickListener {
            val ip = etInput.text.toString().trim()
            if (ip.isNotEmpty()) {
                lanDiscoveryManager.sendUnicastAnnounce(ip)
                if (selectedTransferItems.isNotEmpty()) {
                    val first = selectedTransferItems.first()
                    showTransferSessionOverlay(true, first.name, first.sizeBytes)
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

        val speedFmt = DecimalFormat("0.0")
        val labels = records.map { rec ->
            val statusTag = if (rec.isPartial) "[KISMİ KORUNDU]" else "[TAMAMLANDI]"
            val durStr = formatElapsedSeconds(rec.elapsedMillis)
            "$statusTag ${rec.fileName} (${StorageHelper.formatFileSize(rec.bytesWritten)} • $durStr • ${speedFmt.format(rec.avgSpeedMbps)} MB/s)"
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
                hideOverlaySmoothly(binding.overlayTransferSession)
                Toast.makeText(this, "Aktarım geçmişi temizlendi.", Toast.LENGTH_SHORT).show()
            }
            .setPositiveButton("Kapat", null)
            .show()
    }

    private fun showNetworkAndDeviceInfoDialog() {
        val ips = lanDiscoveryManager.getLocalIpAddresses()
        val ipText = if (ips.isEmpty()) "Wi-Fi Direct P2P Hazır" else ips.joinToString(", ")
        val hwBands = wifiDirectManager.getHardwareBandCapabilitySummary()
        val msg = buildString {
            appendLine("• Cihaz Adı: $deviceAlias")
            appendLine("• Desteklenen Bantlar: $hwBands")
            appendLine("• Aktif IP Adresleri: $ipText")
            appendLine("• Zero-Copy Motoru: 32 MB Çift Çekirdekli DMA + Otomatik 5GHz P2P")
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
        val ips = lanDiscoveryManager.getLocalIpAddresses().joinToString(", ").ifEmpty { "Çevrimdışı P2P Modu" }
        val msg = buildString {
            appendLine("• Donanım Bant Desteği: $hwBands")
            appendLine("• Aktif Ağ Durumu: $ips")
            appendLine()
            append("İki cihaz aynı ev Wi-Fi ağındayken veya hiçbir ağa bağlı değilken bile hedef cihaza dokunduğunuzda uygulama otomatik olarak 5 GHz / 6 GHz doğrudan P2P köprüsü kurar ve maksimum donanım hızında aktarır.")
        }
        AlertDialog.Builder(this)
            .setTitle("Sorun Giderme")
            .setMessage(msg)
            .setPositiveButton("P2P & Ağ Taramasını Sıfırla") { _: DialogInterface, _: Int ->
                wifiDirectManager.cleanupStaleGroups {
                    wifiDirectManager.syncStateAndDiscover()
                    lanDiscoveryManager.broadcastDiscoveryBeacon(isPing = true)
                }
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
        p2pWaitClientJob?.cancel()
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
