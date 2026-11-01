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
import java.util.concurrent.atomic.AtomicInteger
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
    private val onDirectDisconnected: () -> Unit = {},
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
    private var negotiatingUntilMs: Long = 0L

    @Volatile
    private var isTransferCurrentlyRunning: Boolean = false

    @Volatile
    private var activeBandLabel: String = "Doğrudan Köprü (6GHz / 5GHz)"

    private val lastKnownP2pFreqMhz = AtomicInteger(0)
    private val cachedGroupCredentials = AtomicReference<P2pGroupCredentials?>(null)
    private val discoveredP2pPeers = mutableMapOf<String, PeerDevice>()

    fun shouldSkipP2pBecauseHotspotOrWifiOff(): Boolean {
        if (CompatibilityHelper.isMobileHotspotActive(context)) return true
        if (!CompatibilityHelper.isWifiRadioEnabled(context)) return true
        return false
    }

    fun getActiveGroupCredentials(): P2pGroupCredentials? = cachedGroupCredentials.get()

    /**
     * Resolves the real hardware frequency (e.g. 5745 MHz, 5765 MHz, 5180 MHz) of the current P2P
     * or Hotspot link so the UI always displays the exact frequency in MHz.
     */
    fun resolveCurrentLinkBandLabel(fallbackLabel: String): String {
        cachedGroupCredentials.get()?.let { creds ->
            if (creds.freqMhz >= 2400) {
                return formatBandLabelFromFreq(creds.freqMhz)
            }
        }
        val p2pFreq = lastKnownP2pFreqMhz.get()
        if (p2pFreq >= 2400) {
            return formatBandLabelFromFreq(p2pFreq)
        }
        val mgr = p2pManager
        val ch = channel
        if (mgr != null && ch != null && Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            val latch = CountDownLatch(1)
            mgr.requestGroupInfo(ch) { group ->
                if (group != null && group.frequency >= 2400) {
                    lastKnownP2pFreqMhz.set(group.frequency)
                }
                latch.countDown()
            }
            try {
                latch.await(120L, TimeUnit.MILLISECONDS)
            } catch (_: Throwable) {
            }
            val queried = lastKnownP2pFreqMhz.get()
            if (queried >= 2400) {
                return formatBandLabelFromFreq(queried)
            }
        }
        val wifiFreq = CompatibilityHelper.getActiveWifiFrequencyMhz(context)
        if (wifiFreq >= 2400) {
            return formatBandLabelFromFreq(wifiFreq)
        }
        return fallbackLabel
    }

    private fun formatBandLabelFromFreq(freqMhz: Int): String {
        return when {
            freqMhz >= 5925 -> "6 GHz Doğrudan Köprü ($freqMhz MHz)"
            freqMhz >= 5000 -> "5 GHz Doğrudan Köprü ($freqMhz MHz)"
            freqMhz >= 2400 -> "2.4 GHz Doğrudan Köprü ($freqMhz MHz)"
            else -> activeBandLabel
        }
    }

    fun setTransferInProgress(inProgress: Boolean) {
        isTransferCurrentlyRunning = inProgress
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
        if (inProgress) {
            try {
                mgr.stopPeerDiscovery(ch, null)
            } catch (_: Throwable) {
            }
        } else {
            negotiatingUntilMs = 0L
            if (CompatibilityHelper.getLocalP2pInterfaceIp() == null) {
                discoverPeers()
            }
        }
    }

    private fun isBusyNegotiating(): Boolean {
        return System.currentTimeMillis() < negotiatingUntilMs
    }

    private val p2pReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context?, intent: Intent?) {
            if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
            val action = intent?.action ?: return
            when (action) {
                WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION -> {
                    val state = intent.getIntExtra(
                        WifiP2pManager.EXTRA_WIFI_STATE,
                        WifiP2pManager.WIFI_P2P_STATE_DISABLED
                    )
                    if (state == WifiP2pManager.WIFI_P2P_STATE_ENABLED && !isTransferCurrentlyRunning) {
                        discoverPeers()
                    }
                }
                WifiP2pManager.WIFI_P2P_DISCOVERY_CHANGED_ACTION -> {
                    val state = intent.getIntExtra(
                        WifiP2pManager.EXTRA_DISCOVERY_STATE,
                        WifiP2pManager.WIFI_P2P_DISCOVERY_STOPPED
                    )
                    if (state == WifiP2pManager.WIFI_P2P_DISCOVERY_STOPPED &&
                        !isTransferCurrentlyRunning &&
                        !isBusyNegotiating() &&
                        CompatibilityHelper.getLocalP2pInterfaceIp() == null
                    ) {
                        discoverPeers()
                    }
                }
                WifiP2pManager.WIFI_P2P_PEERS_CHANGED_ACTION -> {
                    val listFromIntent = CompatibilityHelper.getParcelableExtraCompat(
                        intent,
                        WifiP2pManager.EXTRA_P2P_DEVICE_LIST,
                        WifiP2pDeviceList::class.java
                    )
                    if (listFromIntent != null) {
                        handlePeerList(listFromIntent)
                    }
                    val ch = channel ?: return
                    p2pManager?.requestPeers(ch) { peerList: WifiP2pDeviceList? ->
                        if (peerList != null) {
                            handlePeerList(peerList)
                        }
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
                            negotiatingUntilMs = 0L
                            lastKnownP2pFreqMhz.set(0)
                            cachedGroupCredentials.set(null)
                            onDirectDisconnected()
                            if (!isTransferCurrentlyRunning) {
                                discoverPeers()
                            }
                        }
                    }
                }
            }
        }
    }

    private fun handleGroupFormedInfo(ch: WifiP2pManager.Channel, p2pInfo: WifiP2pInfo) {
        negotiatingUntilMs = 0L
        val ownerIp = p2pInfo.groupOwnerAddress?.hostAddress ?: "192.168.49.1"
        p2pManager?.requestGroupInfo(ch) { group: WifiP2pGroup? ->
            val freqMhz = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && group != null) {
                group.frequency
            } else {
                0
            }
            if (freqMhz >= 2400) {
                lastKnownP2pFreqMhz.set(freqMhz)
            }
            val bandDesc = formatBandLabelFromFreq(freqMhz)
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
        syncStateAndDiscover()
    }

    fun syncStateAndDiscover() {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return
        val mgr = p2pManager ?: return
        val ch = channel ?: return

        mgr.requestConnectionInfo(ch) { info ->
            if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                handleGroupFormedInfo(ch, info)
            } else {
                lastKnownP2pFreqMhz.set(0)
                cachedGroupCredentials.set(null)
                onDirectDisconnected()
                if (!isTransferCurrentlyRunning) {
                    discoverPeers()
                }
            }
        }
    }

    private fun startDiscoveryHeartbeat() {
        discoveryHeartbeatJob?.cancel()
        discoveryHeartbeatJob = scope.launch {
            while (isActive) {
                delay(4_000L)
                if (!isTransferCurrentlyRunning &&
                    !isBusyNegotiating() &&
                    !shouldSkipP2pBecauseHotspotOrWifiOff() &&
                    CompatibilityHelper.getLocalP2pInterfaceIp() == null
                ) {
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
        negotiatingUntilMs = 0L
        lastKnownP2pFreqMhz.set(0)
        cachedGroupCredentials.set(null)
        onDirectDisconnected()
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
        if (shouldSkipP2pBecauseHotspotOrWifiOff() || isTransferCurrentlyRunning) return
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            mgr.discoverPeers(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("Otomatik Köprü (${getHardwareBandCapabilitySummary()})")
                    mgr.requestPeers(ch) { peerList ->
                        if (peerList != null) {
                            handlePeerList(peerList)
                        }
                    }
                }
                override fun onFailure(reason: Int) {
                    mgr.requestPeers(ch) { peerList ->
                        if (peerList != null) {
                            handlePeerList(peerList)
                        }
                    }
                }
            })
        } catch (_: Throwable) {
        }
    }

    fun createAutonomousHighSpeedGroupBlocking(timeoutMs: Long = 3500L): P2pGroupCredentials? {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return null
        val mgr = p2pManager ?: return null
        val ch = channel ?: return null

        val existing = cachedGroupCredentials.get()
        if (existing != null &&
            existing.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
            CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1"
        ) {
            return existing
        }

        val initialCheckLatch = CountDownLatch(1)
        mgr.requestGroupInfo(ch) { group ->
            if (group != null && group.isGroupOwner && !group.networkName.isNullOrBlank() && !group.passphrase.isNullOrBlank()) {
                val ssidBytes = group.networkName.toByteArray(Charsets.UTF_8).size
                if (ssidBytes <= 32) {
                    val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) group.frequency else 5180
                    if (freq >= 2400) lastKnownP2pFreqMhz.set(freq)
                    val label = formatBandLabelFromFreq(if (freq > 0) freq else 5180)
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
            }
            initialCheckLatch.countDown()
        }
        try {
            initialCheckLatch.await(200L, TimeUnit.MILLISECONDS)
        } catch (_: Throwable) {
        }
        cachedGroupCredentials.get()?.let {
            if (CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1") {
                return it
            }
        }

        negotiatingUntilMs = System.currentTimeMillis() + timeoutMs + 1500L

        val tag = CompatibilityHelper.getShortDeviceTag(context)
        val shortAsciiSsid = "DIRECT-ZP-Zeph$tag"
        val shortAsciiPwd = "zeph88889999"

        try {
            mgr.stopPeerDiscovery(ch, null)
        } catch (_: Throwable) {
        }

        mainHandler.post {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                try {
                    val supports5G = wifiManager?.is5GHzBandSupported == true
                    val band = if (preferHighBand && supports5G) {
                        WifiP2pConfig.GROUP_OWNER_BAND_5GHZ
                    } else {
                        WifiP2pConfig.GROUP_OWNER_BAND_AUTO
                    }
                    val config5G = WifiP2pConfig.Builder()
                        .setNetworkName(shortAsciiSsid)
                        .setPassphrase(shortAsciiPwd)
                        .setGroupOperatingBand(band)
                        .enablePersistentMode(false)
                        .build()

                    mgr.createGroup(ch, config5G, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                        }
                        override fun onFailure(reason: Int) {
                            try {
                                val configAuto = WifiP2pConfig.Builder()
                                    .setNetworkName(shortAsciiSsid)
                                    .setPassphrase(shortAsciiPwd)
                                    .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_AUTO)
                                    .enablePersistentMode(false)
                                    .build()
                                mgr.createGroup(ch, configAuto, object : WifiP2pManager.ActionListener {
                                    override fun onSuccess() {}
                                    override fun onFailure(r2: Int) {
                                        try {
                                            mgr.createGroup(ch, null)
                                        } catch (_: Throwable) {
                                        }
                                    }
                                })
                            } catch (_: Throwable) {
                                try {
                                    mgr.createGroup(ch, null)
                                } catch (_: Throwable) {
                                }
                            }
                        }
                    })
                } catch (_: Throwable) {
                    try {
                        mgr.createGroup(ch, null)
                    } catch (_: Throwable) {
                    }
                }
            } else {
                try {
                    mgr.createGroup(ch, null)
                } catch (_: Throwable) {
                }
            }
        }

        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val found = cachedGroupCredentials.get()
            if (found != null &&
                found.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
                CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1"
            ) {
                negotiatingUntilMs = 0L
                return found
            }

            val latch = CountDownLatch(1)
            mgr.requestGroupInfo(ch) { group ->
                if (group != null && !group.networkName.isNullOrBlank() && !group.passphrase.isNullOrBlank()) {
                    val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) group.frequency else 5180
                    if (freq >= 2400) lastKnownP2pFreqMhz.set(freq)
                    val label = formatBandLabelFromFreq(if (freq > 0) freq else 5180)
                    val creds = P2pGroupCredentials(
                        ssid = group.networkName,
                        passphrase = group.passphrase,
                        ownerIp = "192.168.49.1",
                        freqMhz = if (freq > 0) freq else 5180,
                        bandLabel = label
                    )
                    if (creds.ssid.toByteArray(Charsets.UTF_8).size <= 32) {
                        cachedGroupCredentials.set(creds)
                    }
                }
                latch.countDown()
            }
            try {
                latch.await(150L, TimeUnit.MILLISECONDS)
            } catch (_: Throwable) {
            }
            val readyCreds = cachedGroupCredentials.get()
            if (readyCreds != null &&
                readyCreds.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
                CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1"
            ) {
                negotiatingUntilMs = 0L
                return readyCreds
            }
            try {
                Thread.sleep(90L)
            } catch (_: Throwable) {
                break
            }
        }
        negotiatingUntilMs = 0L
        val finalCreds = cachedGroupCredentials.get()
        return if (finalCreds != null &&
            finalCreds.ssid.toByteArray(Charsets.UTF_8).size <= 32 &&
            CompatibilityHelper.getLocalP2pInterfaceIp() == "192.168.49.1"
        ) {
            finalCreds
        } else {
            null
        }
    }

    /**
     * Silently joins the peer's Autonomous 5 GHz / 6 GHz Group using exact single-frequency scan
     * (setGroupOperatingFrequency) first, falling back to band scan if needed.
     */
    fun joinAutonomousGroupSilentlyBlocking(
        creds: P2pGroupCredentials,
        timeoutMs: Long = 3400L
    ): Boolean {
        if (shouldSkipP2pBecauseHotspotOrWifiOff()) return false
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.Q) return false
        val mgr = p2pManager ?: return false
        val ch = channel ?: return false

        if (creds.ssid.toByteArray(Charsets.UTF_8).size > 32 || !creds.ssid.startsWith("DIRECT-")) {
            return false
        }

        val currentP2pIp = CompatibilityHelper.getLocalP2pInterfaceIp()
        if (!currentP2pIp.isNullOrBlank() && currentP2pIp != "192.168.49.1") {
            if (creds.freqMhz >= 2400) lastKnownP2pFreqMhz.set(creds.freqMhz)
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
            // Wait until the old 192.168.49.1 interface is completely torn down before calling connect()
            for (w in 0 until 8) {
                if (CompatibilityHelper.getLocalP2pInterfaceIp() == null) break
                try { Thread.sleep(70L) } catch (_: Throwable) {}
            }
        }

        negotiatingUntilMs = System.currentTimeMillis() + timeoutMs + 1000L
        activeBandLabel = creds.bandLabel
        if (creds.freqMhz >= 2400) {
            lastKnownP2pFreqMhz.set(creds.freqMhz)
        }

        try {
            mgr.stopPeerDiscovery(ch, null)
        } catch (_: Throwable) {
        }

        mainHandler.post {
            try {
                val band = if (creds.freqMhz >= 5000 && wifiManager?.is5GHzBandSupported == true) {
                    WifiP2pConfig.GROUP_OWNER_BAND_5GHZ
                } else {
                    WifiP2pConfig.GROUP_OWNER_BAND_AUTO
                }

                // Try exact frequency first (setGroupOperatingFrequency scans ONLY that 1 channel -> ~200ms join!)
                val exactFreqConfig = if (creds.freqMhz >= 2412) {
                    try {
                        WifiP2pConfig.Builder()
                            .setNetworkName(creds.ssid)
                            .setPassphrase(creds.passphrase)
                            .setGroupOperatingFrequency(creds.freqMhz)
                            .build()
                    } catch (_: Throwable) {
                        null
                    }
                } else {
                    null
                }

                val bandJoinConfig = WifiP2pConfig.Builder()
                    .setNetworkName(creds.ssid)
                    .setPassphrase(creds.passphrase)
                    .setGroupOperatingBand(band)
                    .build()

                val primaryConfig = exactFreqConfig ?: bandJoinConfig

                mgr.connect(ch, primaryConfig, object : WifiP2pManager.ActionListener {
                    override fun onSuccess() {
                    }
                    override fun onFailure(reason: Int) {
                        try {
                            mgr.connect(ch, bandJoinConfig, object : WifiP2pManager.ActionListener {
                                override fun onSuccess() {}
                                override fun onFailure(r2: Int) {
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
                })
            } catch (_: Throwable) {
            }
        }

        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val p2pIp = CompatibilityHelper.getLocalP2pInterfaceIp()
            if (!p2pIp.isNullOrBlank() && p2pIp != "192.168.49.1") {
                negotiatingUntilMs = 0L
                return true
            }
            try {
                Thread.sleep(85L)
            } catch (_: Throwable) {
                break
            }
        }

        negotiatingUntilMs = 0L
        try {
            mgr.cancelConnect(ch, null)
        } catch (_: Throwable) {
        }
        try {
            mgr.stopPeerDiscovery(ch, null)
        } catch (_: Throwable) {
        }
        return false
    }

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

        negotiatingUntilMs = System.currentTimeMillis() + 14_000L

        mgr.requestConnectionInfo(ch) { existingInfo ->
            if (existingInfo != null && existingInfo.groupFormed && existingInfo.groupOwnerAddress != null) {
                onConnectedOrFailed(true, activeBandLabel)
                handleGroupFormedInfo(ch, existingInfo)
                return@requestConnectionInfo
            }

            if (!peerSsid.isNullOrBlank() &&
                !peerPassphrase.isNullOrBlank() &&
                peerSsid.toByteArray(Charsets.UTF_8).size <= 32 &&
                Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q
            ) {
                scope.launch {
                    val creds = P2pGroupCredentials(
                        ssid = peerSsid,
                        passphrase = peerPassphrase,
                        ownerIp = "192.168.49.1",
                        freqMhz = if (peerFreqMhz > 0) peerFreqMhz else 5180,
                        bandLabel = formatBandLabelFromFreq(if (peerFreqMhz > 0) peerFreqMhz else 5180)
                    )
                    val joined = joinAutonomousGroupSilentlyBlocking(creds, 3200L)
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
            groupOwnerIntent = 0
        }

        try {
            mgr.connect(ch, config, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onConnectedOrFailed(true, activeBandLabel)
                    startFastConnectionCompletionPoller(mgr, ch)
                }
                override fun onFailure(reason: Int) {
                    try {
                        mgr.cancelConnect(ch, object : WifiP2pManager.ActionListener {
                            override fun onSuccess() {
                                mgr.connect(ch, config, object : WifiP2pManager.ActionListener {
                                    override fun onSuccess() {
                                        onConnectedOrFailed(true, activeBandLabel)
                                        startFastConnectionCompletionPoller(mgr, ch)
                                    }
                                    override fun onFailure(r2: Int) {
                                        negotiatingUntilMs = 0L
                                        discoverPeers()
                                        onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $r2)")
                                    }
                                })
                            }
                            override fun onFailure(r1: Int) {
                                negotiatingUntilMs = 0L
                                discoverPeers()
                                onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $reason)")
                            }
                        })
                    } catch (_: Throwable) {
                        negotiatingUntilMs = 0L
                        onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $reason)")
                    }
                }
            })
        } catch (t: Throwable) {
            negotiatingUntilMs = 0L
            onConnectedOrFailed(false, t.message ?: "Bilinmeyen hata")
        }
    }

    private fun startFastConnectionCompletionPoller(mgr: WifiP2pManager, ch: WifiP2pManager.Channel) {
        connectionPollJob?.cancel()
        connectionPollJob = scope.launch {
            for (i in 0 until 55) {
                if (!isActive) break
                delay(220L)
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
            negotiatingUntilMs = 0L
        }
    }

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

    private fun handlePeerList(peerList: WifiP2pDeviceList?) {
        val list = peerList?.deviceList ?: emptyList()
        discoveredP2pPeers.clear()

        for (device: WifiP2pDevice in list) {
            val addr = device.deviceAddress ?: continue
            val rawDeviceName = device.deviceName?.trim().orEmpty()
            val resolvedName = if (rawDeviceName.isNotBlank() && !CompatibilityHelper.isPlaceholderDeviceName(rawDeviceName)) {
                CompatibilityHelper.mapRawModelToFriendlyName("", rawDeviceName)
            } else {
                "Galaxy / Android (${addr.takeLast(5)})"
            }

            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = resolvedName,
                deviceModel = "Wi-Fi Direct",
                ipAddress = if (device.status == WifiP2pDevice.CONNECTED && device.isGroupOwner) "192.168.49.1" else null,
                macAddress = addr,
                port = 53317,
                category = DeviceCategory.PHONE,
                transport = ConnectionTransport.WIFI_DIRECT_AUTO
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
