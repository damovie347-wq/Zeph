package com.fasttransfer.p2p.network

import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.NetworkInfo
import android.net.wifi.WifiManager
import android.net.wifi.WpsInfo
import android.net.wifi.p2p.WifiP2pConfig
import android.net.wifi.p2p.WifiP2pDevice
import android.net.wifi.p2p.WifiP2pDeviceList
import android.net.wifi.p2p.WifiP2pGroup
import android.net.wifi.p2p.WifiP2pInfo
import android.net.wifi.p2p.WifiP2pManager
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceInfo
import android.net.wifi.p2p.nsd.WifiP2pDnsSdServiceRequest
import android.os.Build
import android.os.Handler
import android.os.Looper
import com.fasttransfer.p2p.model.ConnectionTransport
import com.fasttransfer.p2p.model.DeviceCategory
import com.fasttransfer.p2p.model.P2pGroupCredentials
import com.fasttransfer.p2p.model.PeerDevice
import com.fasttransfer.p2p.util.CompatibilityHelper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.io.BufferedReader
import java.io.FileReader
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

@SuppressLint("MissingPermission")
class CascadingWifiDirectManager(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val onPeersUpdated: (List<PeerDevice>) -> Unit,
    private val onDirectConnectionReady: (
        groupOwnerIp: String,
        isGroupOwner: Boolean,
        bandLabel: String
    ) -> Unit,
    private val onStatusLog: (String) -> Unit
) {
    private val wifiManager: WifiManager? =
        context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
    private val p2pManager: WifiP2pManager? =
        context.applicationContext.getSystemService(Context.WIFI_P2P_SERVICE) as? WifiP2pManager
    private var channel: WifiP2pManager.Channel? = null
    private val mainHandler = Handler(Looper.getMainLooper())
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private var discoveryHeartbeatJob: Job? = null
    private var connectionPollJob: Job? = null

    private var isReceiverRegistered = false
    var preferHighBand: Boolean = true

    @Volatile
    private var isConnectingOrConnected = false

    @Volatile
    private var activeBandLabel: String = "Doğrudan Köprü (6GHz / 5GHz)"

    private val cachedGroupCredentials = AtomicReference<P2pGroupCredentials?>(null)

    private val discoveredP2pPeers = mutableMapOf<String, PeerDevice>()
    private val dnsSdAliasesByMac = mutableMapOf<String, String>()
    private val dnsSdModelsByMac = mutableMapOf<String, String>()
    private val dnsSdSsidByMac = mutableMapOf<String, String>()
    private val dnsSdPwdByMac = mutableMapOf<String, String>()
    private val dnsSdFreqByMac = mutableMapOf<String, Int>()

    /**
     * Never touch WifiP2pManager when Mobile Hotspot is active or Wi-Fi is disabled!
     * This completely eliminates Samsung One UI's annoying "Turn off Mobile Hotspot to use Wi-Fi Direct" warnings.
     */
    fun shouldSkipP2pBecauseHotspotOrWifiOff(): Boolean {
        if (CompatibilityHelper.isMobileHotspotActive(context)) return true
        if (!CompatibilityHelper.isWifiRadioEnabled(context)) return true
        return false
    }

    fun getActiveGroupCredentials(): P2pGroupCredentials? = cachedGroupCredentials.get()

    private val p2pReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context?, intent: Intent?) {
            if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
            val action = intent?.action ?: return
            when (action) {
                WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION -> {
                    val state = intent.getIntExtra(
                        WifiP2pManager.EXTRA_DISCOVERY_STATE,
                        WifiP2pManager.WIFI_P2P_DISCOVERY_STOPPED
                    )
                    if (state == WifiP2pManager.WIFI_P2P_DISCOVERY_STOPPED && !isConnectingOrConnected) {
                        discoverPeers()
                    }
                }
                WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION -> {
                    val ch = channel ?: return
                    p2pManager?.requestPeers(ch) { peerList: WifiP2pDeviceList? ->
                        handlePeerList(peerList)
                    }
                }
                WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION -> {
                    val ch = channel ?: return
                    val p2pInfo = CompatibilityHelper.getParcelableExtraCompat(
                        intent,
                        WifiP2pManager.EXTRA_WIFI_P2P_INFO,
                        WifiP2pInfo::class.java
                    )
                    if (p2pInfo != null && p2pInfo.groupFormed && p2pInfo.groupOwnerAddress != null) {
                        handleGroupFormedInfo(ch, p2pInfo)
                    } else {
                        @Suppress("DEPRECATION")
                        val networkInfo = CompatibilityHelper.getParcelableExtraCompat(
                            intent,
                            WifiP2pManager.EXTRA_NETWORK_INFO,
                            NetworkInfo::class.java
                        )
                        @Suppress("DEPRECATION")
                        if (networkInfo?.isConnected == true) {
                            p2pManager?.requestConnectionInfo(ch) { info ->
                                if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                                    handleGroupFormedInfo(ch, info)
                                }
                            }
                        } else {
                            isConnectingOrConnected = false
                            cachedGroupCredentials.set(null)
                            discoverPeers()
                        }
                    }
                }
            }
        }
    }

    private fun handleGroupFormedInfo(ch: WifiP2pManager.Channel, p2pInfo: WifiP2pInfo) {
        isConnectingOrConnected = true
        val ownerIp = p2pInfo.groupOwnerAddress?.hostAddress ?: "192.168.49.1"
        p2pManager?.requestGroupInfo(ch) { group: WifiP2pGroup? ->
            val freqMhz = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && group != null) {
                group.frequency
            } else {
                0
            }
            val bandDesc = when {
                freqMhz >= 5925 -> "6 GHz Doğrudan Köprü ($freqMhz MHz)"
                freqMhz >= 5000 -> "5 GHz Doğrudan Köprü ($freqMhz MHz)"
                freqMhz >= 2400 -> "2.4 GHz Doğrudan Köprü ($freqMhz MHz)"
                else -> activeBandLabel
            }
            activeBandLabel = bandDesc

            if (group != null && p2pInfo.isGroupOwner) {
                val ssid = group.networkName ?: ""
                val pwd = group.passphrase ?: ""
                if (ssid.isNotEmpty() && pwd.isNotEmpty()) {
                    cachedGroupCredentials.set(
                        P2pGroupCredentials(
                            ssid = ssid,
                            passphrase = pwd,
                            ownerIp = ownerIp,
                            freqMhz = if (freqMhz > 0) freqMhz else 5180,
                            bandLabel = bandDesc
                        )
                    )
                }
            }
            onDirectConnectionReady(ownerIp, p2pInfo.isGroupOwner, bandDesc)
        }
    }

    fun start() {
        if (p2pManager == null) return
        if (channel == null) {
            channel = p2pManager.initialize(context, Looper.getMainLooper(), null)
        }
        if (!isReceiverRegistered) {
            val filter = IntentFilter().apply {
                addAction(WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION)
                addAction(WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION)
                addAction(WifiP2pManager.WIFI_P2P_CONNECTION_CHANGED_ACTION)
                addAction(WifiP2pManager.WIFI_P2P_THIS_DEVICE_CHANGED_ACTION)
                addAction(WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION)
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                context.registerReceiver(p2pReceiver, filter, Context.RECEIVER_NOT_EXPORTED)
            } else {
                context.registerReceiver(p2pReceiver, filter)
            }
            isReceiverRegistered = true
        }

        startDiscoveryHeartbeat()
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) {
            return
        }

        // Check if a P2P group is ALREADY formed (e.g. from an active session); if not, start discovery immediately!
        val ch = channel ?: return
        p2pManager.requestConnectionInfo(ch) { info ->
            if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                handleGroupFormedInfo(ch, info)
            } else {
                registerLocalP2pService()
                discoverPeers()
            }
        }
    }

    fun registerLocalP2pService() {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            val creds = cachedGroupCredentials.get()
            val record = mutableMapOf(
                "alias" to getLocalAlias(),
                "model" to CompatibilityHelper.mapRawModelToFriendlyName(Build.MANUFACTURER, Build.MODEL)
            )
            if (creds != null) {
                record["ssid"] = creds.ssid
                record["pwd"] = creds.passphrase
                record["freq"] = creds.freqMhz.toString()
            }
            val serviceInfo = WifiP2pDnsSdServiceInfo.newInstance(
                "ZephP2P",
                "_fasttransfer._tcp",
                record
            )
            mgr.clearLocalServices(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    mgr.addLocalService(ch, serviceInfo, null)
                }
                override fun onFailure(reason: Int) {
                    mgr.addLocalService(ch, serviceInfo, null)
                }
            })

            val servListener = WifiP2pManager.DnsSdServiceResponseListener { _, _, srcDevice ->
                if (srcDevice != null && !srcDevice.deviceAddress.isNullOrBlank()) {
                    onDnsSdPeerFound(srcDevice, null)
                }
            }
            val txtListener = WifiP2pManager.DnsSdTxtRecordListener { _, txtRecordMap, srcDevice ->
                if (srcDevice != null && !srcDevice.deviceAddress.isNullOrBlank()) {
                    onDnsSdPeerFound(srcDevice, txtRecordMap)
                }
            }
            mgr.setDnsSdResponseListeners(ch, servListener, txtListener)
            val serviceRequest = WifiP2pDnsSdServiceRequest.newInstance()
            mgr.clearServiceRequests(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    mgr.addServiceRequest(ch, serviceRequest, null)
                }
                override fun onFailure(reason: Int) {
                    mgr.addServiceRequest(ch, serviceRequest, null)
                }
            })
        } catch (_: Throwable) {
        }
    }

    /**
     * Fast 5-second non-blocking discovery loop so offline devices always find each other
     * within 1-3 seconds without ever opening Android Settings -> Wi-Fi Direct!
     */
    private fun startDiscoveryHeartbeat() {
        discoveryHeartbeatJob?.cancel()
        discoveryHeartbeatJob = scope.launch {
            while (isActive) {
                delay(5_000L)
                if (!isConnectingOrConnected && !shouldSkipP2pBecauseHotspotOrWifiOff()) {
                    discoverPeers()
                }
            }
        }
    }

    fun cleanupStaleGroups(onComplete: (() -> Unit)? = null) {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) {
            onComplete?.invoke()
            return
        }
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onComplete?.invoke()
            return
        }
        isConnectingOrConnected = false
        cachedGroupCredentials.set(null)
        try {
            mgr.cancelConnect(ch, null)
        } catch (_: Throwable) {
        }
        try {
            mgr.removeGroup(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onComplete?.invoke()
                }
                override fun onFailure(reason: Int) {
                    onComplete?.invoke()
                }
            })
        } catch (_: Throwable) {
            onComplete?.invoke()
        }
    }

    fun discoverPeers() {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            mgr.discoverPeers(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("Otomatik Köprü (${getHardwareBandCapabilitySummary()})")
                    mgr.requestPeers(ch) { peerList ->
                        handlePeerList(peerList)
                    }
                }
                override fun onFailure(reason: Int) {
                }
            })
            mgr.discoverServices(ch, null)
        } catch (_: Throwable) {
        }
    }

    /**
     * Creates an Autonomous 5 GHz / SCC High-Speed Wi-Fi Direct Group on the Receiver
     * and returns its SSID + Passphrase + exact Frequency in MHz so the Sender can tune directly
     * to that single channel (SCC) and join silently with ZERO popup!
     */
    fun createAutonomousHighSpeedGroupBlocking(timeoutMs: Long = 4200L): P2pGroupCredentials? {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return null
        val mgr = p2pManager ?: return null
        val ch = channel ?: return null

        val existing = cachedGroupCredentials.get()
        if (existing != null && CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1") {
            return existing
        }

        // 1. Check if a P2P group is already formed on this device
        val initialCheckLatch = CountDownLatch(1)
        mgr.requestGroupInfo(ch) { group ->
            if (group != null && group.isGroupOwner && !group.networkName.isNullOrBlank() && !group.passphrase.isNullOrBlank()) {
                val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) group.frequency else 5180
                val label = when {
                    freq >= 5925 -> "6 GHz Doğrudan Köprü ($freq MHz)"
                    freq >= 5000 -> "5 GHz Doğrudan Köprü ($freq MHz)"
                    else -> "Doğrudan P2P Köprü ($freq MHz)"
                }
                cachedGroupCredentials.set(
                    P2pGroupCredentials(
                        ssid = group.networkName,
                        passphrase = group.passphrase,
                        ownerIp = "192.168.49.1",
                        freqMhz = if (freq > 0) freq else 5180,
                        bandLabel = label
                    )
                )
            }
            initialCheckLatch.countDown()
        }
        try {
            initialCheckLatch.await(300L, TimeUnit.MILLISECONDS)
        } catch (_: Throwable) {
        }
        cachedGroupCredentials.get()?.let { return it }

        // 2. Check current Wi-Fi STA frequency for Single Channel Concurrency (SCC)
        @Suppress("DEPRECATION")
        val currentStaFreq = try {
            wifiManager?.connectionInfo?.frequency ?: 0
        } catch (_: Throwable) {
            0
        }

        mainHandler.post {
            // If wlan0 is already on 5 GHz (e.g. 5180 - 5825 MHz), standard mgr.createGroup(ch, null)
            // immediately creates an SCC (Single Channel Concurrency) 5 GHz P2P Group on that exact channel in ~400ms!
            // If wlan0 is on 2.4 GHz or disconnected, try explicit 5 GHz band first on API 29+, with instant fallback to standard createGroup!
            var triedBuilder = false
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q &&
                preferHighBand &&
                currentStaFreq < 5000 &&
                wifiManager?.is5GHzBandSupported == true
            ) {
                try {
                    val tag = CompatibilityHelper.getShortDeviceTag(context)
                    val config = WifiP2pConfig.Builder()
                        .setNetworkName("DIRECT-ZP-Zeph$tag")
                        .setPassphrase("zeph88889999")
                        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
                        .enablePersistentMode(false)
                        .build()
                    triedBuilder = true
                    mgr.createGroup(ch, config, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                            isConnectingOrConnected = true
                        }
                        override fun onFailure(reason: Int) {
                            // Immediate fallback to SCC native createGroup!
                            try {
                                mgr.createGroup(ch, object : WifiP2pManager.ActionListener {
                                    override fun onSuccess() { isConnectingOrConnected = true }
                                    override fun onFailure(r: Int) {}
                                })
                            } catch (_: Throwable) {
                            }
                        }
                    })
                } catch (_: Throwable) {
                    triedBuilder = false
                }
            }

            if (!triedBuilder) {
                try {
                    mgr.createGroup(ch, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                            isConnectingOrConnected = true
                        }
                        override fun onFailure(reason: Int) {
                        }
                    })
                } catch (_: Throwable) {
                }
            }
        }

        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val found = cachedGroupCredentials.get()
            if (found != null && CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1") {
                return found
            }

            val latch = CountDownLatch(1)
            mgr.requestGroupInfo(ch) { group ->
                if (group != null && !group.networkName.isNullOrBlank() && !group.passphrase.isNullOrBlank()) {
                    val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) group.frequency else 5180
                    val label = when {
                        freq >= 5925 -> "6 GHz Doğrudan Köprü ($freq MHz)"
                        freq >= 5000 -> "5 GHz Doğrudan Köprü ($freq MHz)"
                        freq >= 2400 -> "Doğrudan P2P Köprü ($freq MHz)"
                        else -> "5 GHz Doğrudan Köprü"
                    }
                    val creds = P2pGroupCredentials(
                        ssid = group.networkName,
                        passphrase = group.passphrase,
                        ownerIp = "192.168.49.1",
                        freqMhz = if (freq > 0) freq else 5180,
                        bandLabel = label
                    )
                    cachedGroupCredentials.set(creds)
                }
                latch.countDown()
            }
            try {
                latch.await(180L, TimeUnit.MILLISECONDS)
            } catch (_: Throwable) {
            }
            val readyCreds = cachedGroupCredentials.get()
            if (readyCreds != null && CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1") {
                return readyCreds
            }
            try {
                Thread.sleep(110L)
            } catch (_: Throwable) {
                break
            }
        }
        return cachedGroupCredentials.get()
    }

    /**
     * Silently joins the Receiver's Autonomous Group using its exact SSID, Passphrase, and Single Channel Frequency!
     * Passing setGroupOperatingFrequency(creds.freqMhz) tunes directly to the Receiver's exact channel in 50ms
     * without scanning 25 DFS/5GHz channels, and requires ZERO user password or system dialog!
     */
    fun joinAutonomousGroupSilentlyBlocking(
        creds: P2pGroupCredentials,
        timeoutMs: Long = 5800L
    ): Boolean {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return false
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) return false
        val mgr = p2pManager ?: return false
        val ch = channel ?: return false

        val currentP2pIp = CompatibilityHelper.getLocalP2pInterfaceIp()
        if (!currentP2pIp.isNullOrBlank() && currentP2pIp != "192.168.49.1") {
            return true
        }

        if (currentP2pIp == "192.168.49.1") {
            val rmLatch = CountDownLatch(1)
            try {
                mgr.removeGroup(ch, object : WifiP2pManager.ActionListener {
                    override fun onSuccess() { rmLatch.countDown() }
                    override fun onFailure(reason: Int) { rmLatch.countDown() }
                })
                rmLatch.await(450L, TimeUnit.MILLISECONDS)
            } catch (_: Throwable) {
            }
        }

        isConnectingOrConnected = true
        activeBandLabel = creds.bandLabel

        mainHandler.post {
            try {
                // First try tuning directly to the exact operating frequency (SCC fast path!)
                val exactFreqConfig = if (creds.freqMhz > 0) {
                    WifiP2pConfig.Builder()
                        .setNetworkName(creds.ssid)
                        .setPassphrase(creds.passphrase)
                        .setGroupOperatingFrequency(creds.freqMhz)
                        .build()
                } else {
                    WifiP2pConfig.Builder()
                        .setNetworkName(creds.ssid)
                        .setPassphrase(creds.passphrase)
                        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_AUTO)
                        .build()
                }

                mgr.connect(ch, exactFreqConfig, object : WifiP2pManager.ActionListener {
                    override fun onSuccess() {
                        isConnectingOrConnected = true
                    }
                    override fun onFailure(reason: Int) {
                        try {
                            val autoJoinConfig = WifiP2pConfig.Builder()
                                .setNetworkName(creds.ssid)
                                .setPassphrase(creds.passphrase)
                                .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_AUTO)
                                .build()
                            mgr.connect(ch, autoJoinConfig, null)
                        } catch (_: Throwable) {
                        }
                    }
                })
            } catch (_: Throwable) {
            }
        }

        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val p2pIp = CompatibilityHelper.getLocalP2pInterfaceIp()
            if (!p2pIp.isNullOrBlank() && p2pIp != "192.168.49.1") {
                return true
            }
            try {
                Thread.sleep(120L)
            } catch (_: Throwable) {
                break
            }
        }
        return false
    }

    /**
     * Offline / Direct Wi-Fi Direct Connection when the user taps a discovered peer in Zeph:
     * 1) If already connected to a P2P group, fires onDirectConnectionReady immediately in 0ms!
     * 2) If the peer advertised an Autonomous Group via DNS-SD, joins it silently via SSID+Passphrase!
     * 3) Otherwise, uses the standard WifiP2pConfig (WpsInfo.PBC + groupOwnerIntent = 14) which is the EXACT
     *    same call as Android Settings -> Wi-Fi Direct and automatically negotiates 5 GHz on Samsung One UI!
     */
    fun connectWithCascadingBandFallback(
        macAddress: String,
        peerSsid: String? = null,
        peerPassphrase: String? = null,
        peerFreqMhz: Int = 5180,
        onConnectedOrFailed: (Boolean, String) -> Unit
    ) {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) {
            onConnectedOrFailed(false, "Hotspot / Yerel kanal aktif")
            return
        }
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onConnectedOrFailed(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        isConnectingOrConnected = true

        // 1. Check if already connected to a P2P group!
        mgr.requestConnectionInfo(ch) { existingInfo ->
            if (existingInfo != null && existingInfo.groupFormed && existingInfo.groupOwnerAddress != null) {
                onConnectedOrFailed(true, activeBandLabel)
                handleGroupFormedInfo(ch, existingInfo)
                return@requestConnectionInfo
            }

            // 2. If the peer advertised an active Autonomous Group via DNS-SD, join it silently!
            val targetSsid = peerSsid ?: dnsSdSsidByMac[macAddress]
            val targetPwd = peerPassphrase ?: dnsSdPwdByMac[macAddress]
            val targetFreq = if (peerFreqMhz > 0) peerFreqMhz else (dnsSdFreqByMac[macAddress] ?: 5180)
            if (!targetSsid.isNullOrBlank() && !targetPwd.isNullOrBlank() && Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                scope.launch {
                    val creds = P2pGroupCredentials(
                        ssid = targetSsid,
                        passphrase = targetPwd,
                        ownerIp = "192.168.49.1",
                        freqMhz = targetFreq,
                        bandLabel = if (targetFreq >= 5000) "5 GHz Doğrudan Köprü ($targetFreq MHz)" else "Doğrudan Köprü"
                    )
                    val joined = joinAutonomousGroupSilentlyBlocking(creds, 4200L)
                    if (joined) {
                        mainHandler.post {
                            onConnectedOrFailed(true, creds.bandLabel)
                            onDirectConnectionReady("192.168.49.1", false, creds.bandLabel)
                        }
                        return@launch
                    } else {
                        mainHandler.post {
                            initiateStandardHighSpeedP2pConnect(mgr, ch, macAddress, onConnectedOrFailed)
                        }
                    }
                }
                return@requestConnectionInfo
            }

            // 3. Initiate standard high-speed P2P GO negotiation (identical to Android Settings -> Wi-Fi Direct!)
            initiateStandardHighSpeedP2pConnect(mgr, ch, macAddress, onConnectedOrFailed)
        }
    }

    private fun initiateStandardHighSpeedP2pConnect(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        macAddress: String,
        onConnectedOrFailed: (Boolean, String) -> Unit
    ) {
        val supports5G = wifiManager?.is5GHzBandSupported == true
        activeBandLabel = if (supports5G) "5 GHz Doğrudan Köprü" else "Doğrudan Köprü (P2P)"

        val config = WifiP2pConfig().apply {
            deviceAddress = macAddress
            wps.setup = WpsInfo.PBC
            // High groupOwnerIntent (14) on 5GHz-capable devices ensures wpa_supplicant selects a 5 GHz operating channel!
            groupOwnerIntent = if (preferHighBand && supports5G) 14 else 7
        }

        try {
            mgr.connect(ch, config, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onConnectedOrFailed(true, activeBandLabel)
                    startFastConnectionCompletionPoller(mgr, ch)
                }
                override fun onFailure(reason: Int) {
                    // Retry once after canceling any stale pending invitation
                    try {
                        mgr.cancelConnect(ch, object : WifiP2pManager.ActionListener {
                            override fun onSuccess() {
                                mgr.connect(ch, config, object : WifiP2pManager.ActionListener {
                                    override fun onSuccess() {
                                        onConnectedOrFailed(true, activeBandLabel)
                                        startFastConnectionCompletionPoller(mgr, ch)
                                    }
                                    override fun onFailure(r2: Int) {
                                        isConnectingOrConnected = false
                                        discoverPeers()
                                        onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $r2)")
                                    }
                                })
                            }
                            override fun onFailure(r1: Int) {
                                isConnectingOrConnected = false
                                discoverPeers()
                                onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $reason)")
                            }
                        })
                    } catch (_: Throwable) {
                        isConnectingOrConnected = false
                        onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $reason)")
                    }
                }
            })
        } catch (t: Throwable) {
            isConnectingOrConnected = false
            onConnectedOrFailed(false, t.message ?: "Bilinmeyen hata")
        }
    }

    /**
     * Polls requestConnectionInfo every 300ms right after mgr.connect() succeeds so we detect
     * group formation immediately without waiting for delayed system broadcasts.
     */
    private fun startFastConnectionCompletionPoller(mgr: WifiP2pManager, ch: WifiP2pManager.Channel) {
        connectionPollJob?.cancel()
        connectionPollJob = scope.launch {
            for (i in 0 until 50) {
                if (!isActive) break
                delay(300L)
                val p2pIp = CompatibilityHelper.getLocalP2pInterfaceIp()
                if (!p2pIp.isNullOrBlank()) {
                    mainHandler.post {
                        mgr.requestConnectionInfo(ch) { info ->
                            if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                                handleGroupFormedInfo(ch, info)
                            }
                        }
                    }
                    break
                }
            }
        }
    }

    /**
     * Reads /proc/net/arp to find any connected P2P client IP on 192.168.49.x when this device is Group Owner.
     */
    fun findConnectedP2pClientIpFromArp(): String? {
        try {
            BufferedReader(FileReader("/proc/net/arp")).use { br ->
                var line: String?
                while (br.readLine().also { line = it } != null) {
                    val parts = line!!.trim().split(' ', '\t').filter { it.isNotEmpty() }
                    if (parts.size >= 4) {
                        val ip = parts[0]
                        val flags = parts[2]
                        if (ip.startsWith("192.168.49.") && ip != "192.168.49.1" && flags != "0x0") {
                            return ip
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return null
    }

    private fun onDnsSdPeerFound(device: WifiP2pDevice?, txtRecordMap: Map<String, String>?) {
        val d = device ?: return
        val addr = d.deviceAddress ?: return
        val aliasFromTxt = txtRecordMap?.get("alias")?.trim()
        val modelFromTxt = txtRecordMap?.get("model")?.trim()
        val ssidFromTxt = txtRecordMap?.get("ssid")?.trim()
        val pwdFromTxt = txtRecordMap?.get("pwd")?.trim()
        val freqFromTxt = txtRecordMap?.get("freq")?.trim()?.toIntOrNull() ?: 0

        if (!aliasFromTxt.isNullOrEmpty() && !CompatibilityHelper.isPlaceholderDeviceName(aliasFromTxt)) {
            dnsSdAliasesByMac[addr] = aliasFromTxt
        }
        if (!modelFromTxt.isNullOrEmpty()) {
            dnsSdModelsByMac[addr] = modelFromTxt
        }
        if (!ssidFromTxt.isNullOrEmpty() && !pwdFromTxt.isNullOrEmpty()) {
            dnsSdSsidByMac[addr] = ssidFromTxt
            dnsSdPwdByMac[addr] = pwdFromTxt
            if (freqFromTxt > 0) dnsSdFreqByMac[addr] = freqFromTxt
        }

        val customAlias = dnsSdAliasesByMac[addr]
        val rawDeviceName = d.deviceName?.trim().orEmpty()
        val resolvedName = when {
            !customAlias.isNullOrBlank() -> customAlias
            rawDeviceName.isNotBlank() && !CompatibilityHelper.isPlaceholderDeviceName(rawDeviceName) ->
                CompatibilityHelper.mapRawModelToFriendlyName("", rawDeviceName)
            !dnsSdModelsByMac[addr].isNullOrBlank() -> dnsSdModelsByMac[addr]!!
            else -> "Galaxy / Android (${addr.takeLast(5)})"
        }

        discoveredP2pPeers[addr] = PeerDevice(
            id = "p2p_$addr",
            alias = resolvedName,
            deviceModel = dnsSdModelsByMac[addr] ?: "Wi-Fi Direct",
            ipAddress = if (d.status == WifiP2pDevice.CONNECTED && d.isGroupOwner) "192.168.49.1" else null,
            macAddress = addr,
            port = 53317,
            category = DeviceCategory.PHONE,
            transport = ConnectionTransport.WIFI_DIRECT_AUTO,
            p2pSsid = dnsSdSsidByMac[addr],
            p2pPassphrase = dnsSdPwdByMac[addr],
            p2pFreqMhz = dnsSdFreqByMac[addr] ?: 0
        )
        onPeersUpdated(discoveredP2pPeers.values.toList())
    }

    private fun handlePeerList(peerList: WifiP2pDeviceList?) {
        discoveredP2pPeers.clear()
        val list = peerList?.deviceList ?: emptyList()

        for (device: WifiP2pDevice in list) {
            val addr = device.deviceAddress ?: continue
            val customAlias = dnsSdAliasesByMac[addr]
            val rawDeviceName = device.deviceName?.trim().orEmpty()
            val resolvedName = when {
                !customAlias.isNullOrBlank() -> customAlias
                rawDeviceName.isNotBlank() && !CompatibilityHelper.isPlaceholderDeviceName(rawDeviceName) ->
                    CompatibilityHelper.mapRawModelToFriendlyName("", rawDeviceName)
                !dnsSdModelsByMac[addr].isNullOrBlank() -> dnsSdModelsByMac[addr]!!
                else -> "Galaxy / Android (${addr.takeLast(5)})"
            }

            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = resolvedName,
                deviceModel = dnsSdModelsByMac[addr] ?: "Wi-Fi Direct",
                ipAddress = if (device.status == WifiP2pDevice.CONNECTED && device.isGroupOwner) "192.168.49.1" else null,
                macAddress = addr,
                port = 53317,
                category = DeviceCategory.PHONE,
                transport = ConnectionTransport.WIFI_DIRECT_AUTO,
                p2pSsid = dnsSdSsidByMac[addr],
                p2pPassphrase = dnsSdPwdByMac[addr],
                p2pFreqMhz = dnsSdFreqByMac[addr] ?: 0
            )
        }
        onPeersUpdated(discoveredP2pPeers.values.toList())
    }

    fun getHardwareBandCapabilitySummary(): String {
        val wm = wifiManager ?: return "2.4 GHz Standart"
        return try {
            val has6Ghz = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) wm.is6GHzBandSupported else false
            val has5Ghz = wm.is5GHzBandSupported
            when {
                has6Ghz && has5Ghz -> "6 GHz + 5 GHz + 2.4 GHz"
                has5Ghz -> "5 GHz + 2.4 GHz"
                else -> "2.4 GHz"
            }
        } catch (_: Throwable) {
            "6 GHz / 5 GHz / 2.4 GHz Otomatik"
        }
    }

    fun stop() {
        discoveryHeartbeatJob?.cancel()
        connectionPollJob?.cancel()
        if (!shouldSkipP2pBecauseHotspotOrWifiOff()) {
            cleanupStaleGroups()
        }
        if (isReceiverRegistered) {
            try {
                context.unregisterReceiver(p2pReceiver)
            } catch (_: Throwable) {
            }
            isReceiverRegistered = false
        }
        if (!shouldSkipP2pBecauseHotspotOrWifiOff()) {
            try {
                val ch = channel
                if (ch != null) {
                    p2pManager?.stopPeerDiscovery(ch, null)
                }
            } catch (_: Throwable) {
            }
        }
    }
}
