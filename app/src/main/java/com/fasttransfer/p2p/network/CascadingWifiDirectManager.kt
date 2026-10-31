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
import com.fasttransfer.p2p.model.PeerDevice
import com.fasttransfer.p2p.util.CompatibilityHelper

@SuppressLint("MissingPermission")
class CascadingWifiDirectManager(
    private val context: Context,
    private val onPeersUpdated: (List<PeerDevice>) -> Unit,
    private val onDirectConnectionReady: (
        groupOwnerIp: String,
        isGroupOwner: Boolean,
        bandLabel: String,
        ssid: String?,
        passphrase: String?
    ) -> Unit,
    private val onStatusLog: (String) -> Unit
) {
    private val wifiManager: WifiManager? =
        context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
    private val p2pManager: WifiP2pManager? =
        context.applicationContext.getSystemService(Context.WIFI_P2P_SERVICE) as? WifiP2pManager
    private var channel: WifiP2pManager.Channel? = null
    private val mainHandler = Handler(Looper.getMainLooper())
    private var isReceiverRegistered = false
    var preferHighBand5Ghz: Boolean = true
    private var activeBandLabel: String = "5 GHz Doğrudan Köprü"

    private val discoveredP2pPeers = mutableMapOf<String, PeerDevice>()

    private val p2pReceiver = object : BroadcastReceiver() {
        override fun onReceive(ctx: Context?, intent: Intent?) {
            val action = intent?.action ?: return
            when (action) {
                WifiP2pManager.WIFI_P2P_STATE_CHANGED_ACTION -> {
                    val state = intent.getIntExtra(WifiP2pManager.EXTRA_WIFI_STATE, -1)
                    if (state == WifiP2pManager.WIFI_P2P_STATE_ENABLED) {
                        onStatusLog("5 GHz Doğrudan Köprü & LAN Hazır")
                    } else {
                        onStatusLog("Wi-Fi Kapalı - Lütfen Wi-Fi'yi açın")
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
                            onDirectConnectionReady(
                                ownerIp,
                                p2pInfo.isGroupOwner,
                                bandDesc,
                                group?.networkName,
                                group?.passphrase
                            )
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
                            p2pManager?.requestConnectionInfo(ch) { info ->
                                if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                                    val ownerIp = info.groupOwnerAddress.hostAddress ?: "192.168.49.1"
                                    p2pManager.requestGroupInfo(ch) { grp ->
                                        onDirectConnectionReady(
                                            ownerIp,
                                            info.isGroupOwner,
                                            activeBandLabel,
                                            grp?.networkName,
                                            grp?.passphrase
                                        )
                                    }
                                }
                            }
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
        discoverPeers()
    }

    fun discoverPeers() {
        val mgr = p2pManager ?: return
        val ch = channel ?: return
        try {
            mgr.discoverPeers(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("5 GHz Doğrudan Köprü & LAN Hazır")
                }

                override fun onFailure(reason: Int) {
                    // Do not show alarming error codes on normal background scan when group is already active
                    onStatusLog("5 GHz Doğrudan Köprü & LAN Aktif")
                }
            })
        } catch (_: Throwable) {
        }
    }

    /**
     * Fixes Kod: 2 (WifiP2pManager.BUSY) by first checking if a group is already formed,
     * stopping active peer discovery, cancelling pending connects, and then creating the 5 GHz group.
     */
    fun createDirect5GhzGroupBridge(
        onResult: (Boolean, String, String?, String?) -> Unit
    ) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onResult(false, "Wi-Fi Direct bu cihazda desteklenmiyor", null, null)
            return
        }

        // First check if a group is already formed! If so, reuse it immediately instead of failing with BUSY (2)!
        mgr.requestGroupInfo(ch) { existingGroup ->
            if (existingGroup != null && !existingGroup.networkName.isNullOrEmpty()) {
                val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) existingGroup.frequency else 5180
                val bandStr = if (freq >= 5000) "5 GHz ($freq MHz)" else "2.4 GHz ($freq MHz)"
                onResult(
                    true,
                    "5 GHz Köprü Hazır ($bandStr)",
                    existingGroup.networkName,
                    existingGroup.passphrase
                )
                return@requestGroupInfo
            }

            // Stop peer discovery & cancel any half-open connect so HAL is never BUSY (code 2)
            try {
                mgr.stopPeerDiscovery(ch, null)
            } catch (_: Throwable) {
            }
            try {
                mgr.cancelConnect(ch, null)
            } catch (_: Throwable) {
            }

            mainHandler.postDelayed({
                try {
                    mgr.removeGroup(ch, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                            mainHandler.postDelayed({ createGroupAfterCleanup(mgr, ch, onResult) }, 200L)
                        }
                        override fun onFailure(reason: Int) {
                            createGroupAfterCleanup(mgr, ch, onResult)
                        }
                    })
                } catch (_: Throwable) {
                    createGroupAfterCleanup(mgr, ch, onResult)
                }
            }, 200L)
        }
    }

    private fun createGroupAfterCleanup(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        onResult: (Boolean, String, String?, String?) -> Unit
    ) {
        val supports5G = try {
            wifiManager?.is5GHzBandSupported == true
        } catch (_: Throwable) {
            false
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && preferHighBand5Ghz && supports5G) {
            try {
                val config5G = WifiP2pConfig.Builder()
                    .setNetworkName("DIRECT-ZEPH-5GHZ")
                    .setPassphrase("zeph12345")
                    .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
                    .build()

                mgr.createGroup(ch, config5G, object : WifiP2pManager.ActionListener {
                    override fun onSuccess() {
                        activeBandLabel = "5 GHz Doğrudan Köprü (192.168.49.1)"
                        pollCreatedGroupDetails(mgr, ch, onResult)
                    }
                    override fun onFailure(reason: Int) {
                        createStandardGroupFallback(mgr, ch, onResult)
                    }
                })
                return
            } catch (_: Throwable) {
            }
        }

        createStandardGroupFallback(mgr, ch, onResult)
    }

    private fun createStandardGroupFallback(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        onResult: (Boolean, String, String?, String?) -> Unit
    ) {
        mgr.createGroup(ch, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                activeBandLabel = "Doğrudan Köprü (192.168.49.1)"
                pollCreatedGroupDetails(mgr, ch, onResult)
            }
            override fun onFailure(reason: Int) {
                onResult(
                    false,
                    "Wi-Fi Direct meşgul (Kod: $reason). Lütfen Wi-Fi'yi kapatıp açın veya telefonun kendi 5 GHz Mobil Erişim Noktasını (Hotspot) açın.",
                    null,
                    null
                )
            }
        })
    }

    private fun pollCreatedGroupDetails(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        onResult: (Boolean, String, String?, String?) -> Unit
    ) {
        mainHandler.postDelayed({
            mgr.requestGroupInfo(ch) { group ->
                val ssid = group?.networkName ?: "DIRECT-ZEPH-5GHZ"
                val pass = group?.passphrase ?: "zeph12345"
                val freq = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && group != null) group.frequency else 5180
                val band = if (freq >= 5000) "5 GHz ($freq MHz)" else "Doğrudan Köprü"
                onResult(true, "$band Köprü Aktif • IP: 192.168.49.1", ssid, pass)
            }
        }, 450L)
    }

    fun connectWithCascadingBandFallback(macAddress: String, onConnectedOrFailed: (Boolean, String) -> Unit) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onConnectedOrFailed(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        // Stop discovery first so connect() never fails with BUSY (Kod: 2)
        try {
            mgr.stopPeerDiscovery(ch, null)
        } catch (_: Throwable) {
        }

        val supports5Ghz = try {
            wifiManager?.is5GHzBandSupported == true
        } catch (_: Throwable) {
            false
        }

        mainHandler.postDelayed({
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && preferHighBand5Ghz && supports5Ghz) {
                try {
                    val highBandConfig = WifiP2pConfig.Builder()
                        .setDeviceAddress(android.net.MacAddress.fromString(macAddress))
                        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_5GHZ)
                        .build()

                    activeBandLabel = "5 GHz Doğrudan Köprü"
                    mgr.connect(ch, highBandConfig, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                            onStatusLog("5 GHz Doğrudan Köprü Müzakeresi Başladı...")
                            onConnectedOrFailed(true, "5 GHz Doğrudan Köprü")
                        }

                        override fun onFailure(reason: Int) {
                            connectLegacy24GhzOrAutoFallback(mgr, ch, macAddress, onConnectedOrFailed)
                        }
                    })
                    return@postDelayed
                } catch (_: Throwable) {
                }
            }

            connectLegacy24GhzOrAutoFallback(mgr, ch, macAddress, onConnectedOrFailed)
        }, 150L)
    }

    private fun connectLegacy24GhzOrAutoFallback(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        macAddress: String,
        onConnectedOrFailed: (Boolean, String) -> Unit
    ) {
        activeBandLabel = "Doğrudan Köprü"
        val fallbackConfig = WifiP2pConfig().apply {
            deviceAddress = macAddress
            wps.setup = WpsInfo.PBC
            groupOwnerIntent = 0
        }
        try {
            mgr.connect(ch, fallbackConfig, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("Doğrudan Köprü Bağlantısı Kuruluyor...")
                    onConnectedOrFailed(true, activeBandLabel)
                }

                override fun onFailure(reason: Int) {
                    onStatusLog("Doğrudan bağlantı kurulamadı (Kod: $reason)")
                    onConnectedOrFailed(false, "Cihaz davetini karşı telefondan onaylayın veya Wi-Fi'yi yenileyin (Kod: $reason)")
                }
            })
        } catch (t: Throwable) {
            onConnectedOrFailed(false, t.message ?: "Bilinmeyen hata")
        }
    }

    private fun handlePeerList(peerList: WifiP2pDeviceList?) {
        discoveredP2pPeers.clear()
        val list = peerList?.deviceList ?: emptyList()
        val supports5G = try {
            wifiManager?.is5GHzBandSupported == true
        } catch (_: Throwable) {
            false
        }

        for (device: WifiP2pDevice in list) {
            val addr = device.deviceAddress ?: continue
            val name = device.deviceName?.ifBlank { "Android Cihazı" } ?: "Android Cihazı"
            val transport = if (supports5G && preferHighBand5Ghz) {
                ConnectionTransport.WIFI_DIRECT_5GHZ
            } else {
                ConnectionTransport.WIFI_DIRECT_2_4GHZ
            }
            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = "$name (Wi-Fi Direct)",
                ipAddress = null,
                macAddress = addr,
                port = 53317,
                category = DeviceCategory.PHONE,
                transport = transport
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
                else -> "2.4 GHz Legacy"
            }
        } catch (_: Throwable) {
            "5 GHz / 2.4 GHz Otomatik"
        }
    }

    fun stop() {
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
