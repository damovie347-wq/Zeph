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

    private var isReceiverRegistered = false
    var preferHighBand: Boolean = true
    private var isConnectingOrConnected = false
    private var activeBandLabel: String = "Doğrudan Köprü (6GHz / 5GHz)"

    private val discoveredP2pPeers = mutableMapOf<String, PeerDevice>()
    private val dnsSdAliasesByMac = mutableMapOf<String, String>()

    private val p2pReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context?, intent: Intent?) {
            val action = intent?.action ?: return
            when (action) {
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
                        isConnectingOrConnected = true
                        val ownerIp = p2pInfo.groupOwnerAddress.hostAddress ?: "192.168.49.1"
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
                            onDirectConnectionReady(ownerIp, p2pInfo.isGroupOwner, bandDesc)
                        }
                    } else {
                        @Suppress("DEPRECATION")
                        val networkInfo = CompatibilityHelper.getParcelableExtraCompat(
                            intent,
                            WifiP2pManager.EXTRA_NETWORK_INFO,
                            NetworkInfo::class.java
                        )
                        @Suppress("DEPRECATION")
                        if (networkInfo?.isConnected == true) {
                            isConnectingOrConnected = true
                            p2pManager?.requestConnectionInfo(ch) { info ->
                                if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                                    val ownerIp = info.groupOwnerAddress.hostAddress ?: "192.168.49.1"
                                    onDirectConnectionReady(ownerIp, info.isGroupOwner, activeBandLabel)
                                }
                            }
                        } else {
                            isConnectingOrConnected = false
                        }
                    }
                }
            }
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
            }
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
                context.registerReceiver(p2pReceiver, filter, Context.RECEIVER_NOT_EXPORTED)
            } else {
                context.registerReceiver(p2pReceiver, filter)
            }
            isReceiverRegistered = true
        }

        cleanupStaleGroups {
            registerLocalP2pService()
            discoverPeers()
            startDiscoveryHeartbeat()
        }
    }

    private fun registerLocalP2pService() {
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            val record = mapOf(
                "alias" to getLocalAlias(),
                "model" to (Build.MANUFACTURER ?: "Android")
            )
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

    private fun startDiscoveryHeartbeat() {
        discoveryHeartbeatJob?.cancel()
        discoveryHeartbeatJob = scope.launch {
            while (isActive) {
                delay(14_000L)
                if (!isConnectingOrConnected) {
                    discoverPeers()
                }
            }
        }
    }

    fun cleanupStaleGroups(onComplete: (() -> Unit)? = null) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onComplete?.invoke()
            return
        }
        isConnectingOrConnected = false
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
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            mgr.discoverPeers(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("Otomatik Köprü (${getHardwareBandCapabilitySummary()})")
                }
                override fun onFailure(reason: Int) {
                }
            })
            mgr.discoverServices(ch, null)
        } catch (_: Throwable) {
        }
    }

    /**
     * Automatic Cascading Band Negotiation when Sender taps Receiver:
     * 1) Checks if already connected to a P2P group; if so, triggers immediate transfer.
     * 2) On API 29+, tries GROUP_OWNER_BAND_AUTO (6 GHz / 5 GHz highest shared band) and GROUP_OWNER_BAND_5GHZ.
     * 3) Falls back in 0ms to standard WifiP2pConfig with groupOwnerIntent = 0 (so Receiver becomes 192.168.49.1!).
     */
    fun connectWithCascadingBandFallback(macAddress: String, onConnectedOrFailed: (Boolean, String) -> Unit) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onConnectedOrFailed(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        isConnectingOrConnected = true

        // Check if already connected to a P2P group!
        mgr.requestConnectionInfo(ch) { existingInfo ->
            if (existingInfo != null && existingInfo.groupFormed && existingInfo.groupOwnerAddress != null) {
                val ownerIp = existingInfo.groupOwnerAddress.hostAddress ?: "192.168.49.1"
                onConnectedOrFailed(true, activeBandLabel)
                onDirectConnectionReady(ownerIp, existingInfo.isGroupOwner, activeBandLabel)
                return@requestConnectionInfo
            }

            try {
                mgr.stopPeerDiscovery(ch, null)
            } catch (_: Throwable) {
            }

            mainHandler.postDelayed({
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && preferHighBand) {
                    try {
                        val supports5G = wifiManager?.is5GHzBandSupported == true
                        val bandConstant = if (supports5G) {
                            WifiP2pConfig.GROUP_OWNER_BAND_5GHZ
                        } else {
                            WifiP2pConfig.GROUP_OWNER_BAND_AUTO
                        }
                        val highBandConfig = WifiP2pConfig.Builder()
                            .setDeviceAddress(android.net.MacAddress.fromString(macAddress))
                            .setGroupOperatingBand(bandConstant)
                            .build()

                        activeBandLabel = if (supports5G) "5 GHz Doğrudan Köprü" else "Otomatik Doğrudan Köprü"
                        mgr.connect(ch, highBandConfig, object : WifiP2pManager.ActionListener {
                            override fun onSuccess() {
                                onConnectedOrFailed(true, activeBandLabel)
                            }
                            override fun onFailure(reason: Int) {
                                connectStandardP2pFallback(mgr, ch, macAddress, onConnectedOrFailed)
                            }
                        })
                        return@postDelayed
                    } catch (_: Throwable) {
                    }
                }

                connectStandardP2pFallback(mgr, ch, macAddress, onConnectedOrFailed)
            }, 150L)
        }
    }

    private fun connectStandardP2pFallback(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        macAddress: String,
        onConnectedOrFailed: (Boolean, String) -> Unit
    ) {
        activeBandLabel = "Doğrudan Köprü (P2P)"
        val fallbackConfig = WifiP2pConfig().apply {
            deviceAddress = macAddress
            wps.setup = WpsInfo.PBC
            // groupOwnerIntent = 0 makes the Receiver the Group Owner (192.168.49.1) so Sender knows the IP immediately!
            groupOwnerIntent = 0
        }
        try {
            mgr.connect(ch, fallbackConfig, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onConnectedOrFailed(true, activeBandLabel)
                }
                override fun onFailure(reason: Int) {
                    isConnectingOrConnected = false
                    onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct daveti bekleniyor (Kod: $reason)")
                }
            })
        } catch (t: Throwable) {
            isConnectingOrConnected = false
            onConnectedOrFailed(false, t.message ?: "Bilinmeyen hata")
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
        if (!aliasFromTxt.isNullOrEmpty()) {
            dnsSdAliasesByMac[addr] = aliasFromTxt
        }
        val customAlias = dnsSdAliasesByMac[addr]
        val rawName = customAlias ?: d.deviceName?.ifBlank { "Android Cihazı" } ?: "Android Cihazı"
        discoveredP2pPeers[addr] = PeerDevice(
            id = "p2p_$addr",
            alias = rawName,
            deviceModel = "Wi-Fi Direct",
            ipAddress = if (d.status == WifiP2pDevice.CONNECTED && d.isGroupOwner) "192.168.49.1" else null,
            macAddress = addr,
            port = 53317,
            category = DeviceCategory.PHONE,
            transport = ConnectionTransport.WIFI_DIRECT_AUTO
        )
        onPeersUpdated(discoveredP2pPeers.values.toList())
    }

    private fun handlePeerList(peerList: WifiP2pDeviceList?) {
        discoveredP2pPeers.clear()
        val list = peerList?.deviceList ?: emptyList()

        for (device: WifiP2pDevice in list) {
            val addr = device.deviceAddress ?: continue
            val customAlias = dnsSdAliasesByMac[addr]
            val name = customAlias ?: device.deviceName?.ifBlank { "Android Cihazı" } ?: "Android Cihazı"
            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = name,
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
        cleanupStaleGroups()
        if (isReceiverRegistered) {
            try {
                context.unregisterReceiver(p2pReceiver)
            } catch (_: Throwable) {
            }
            isReceiverRegistered = false
        }
        try {
            val ch = channel
            if (ch != null) {
                p2pManager?.stopPeerDiscovery(ch, null)
            }
        } catch (_: Throwable) {
        }
    }
}
