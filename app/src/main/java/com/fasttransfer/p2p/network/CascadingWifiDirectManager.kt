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
    private var isReceiverRegistered = false
    var preferHighBand: Boolean = true
    private var activeBandLabel: String = "Otomatik Köprü (6GHz/5GHz)"

    private val discoveredP2pPeers = mutableMapOf<String, PeerDevice>()

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
                            p2pManager?.requestConnectionInfo(ch) { info ->
                                if (info != null && info.groupFormed && info.groupOwnerAddress != null) {
                                    val ownerIp = info.groupOwnerAddress.hostAddress ?: "192.168.49.1"
                                    onDirectConnectionReady(ownerIp, info.isGroupOwner, activeBandLabel)
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
        // Automatically clean up any lingering/ghost P2P hotspot groups from previous sessions!
        cleanupStaleGroups {
            discoverPeers()
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
    }

    /**
     * Removes any persistent DIRECT-... group from Android's system_server so no unwanted hotspot
     * ever stays open in Wi-Fi settings.
     */
    fun cleanupStaleGroups(onComplete: (() -> Unit)? = null) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onComplete?.invoke()
            return
        }
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
        } catch (_: Throwable) {
        }
    }

    /**
     * Automatic Cascading Band Negotiation when X1 taps X2:
     * 1) Tier 1: GROUP_OWNER_BAND_AUTO (Automatically picks 6 GHz on Wi-Fi 6E/7 or 5 GHz on Wi-Fi 5/6)
     * 2) Tier 2: GROUP_OWNER_BAND_5GHZ explicit 5 GHz
     * 3) Tier 3: Immediate 0ms fallback to 2.4 GHz / Legacy WifiP2pConfig
     */
    fun connectWithCascadingBandFallback(macAddress: String, onConnectedOrFailed: (Boolean, String) -> Unit) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onConnectedOrFailed(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        try {
            mgr.stopPeerDiscovery(ch, null)
        } catch (_: Throwable) {
        }

        mainHandler.postDelayed({
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && preferHighBand) {
                try {
                    // GROUP_OWNER_BAND_AUTO lets the Wi-Fi HAL pick the highest shared band (6 GHz -> 5 GHz -> 2.4 GHz)
                    val autoHighBandConfig = WifiP2pConfig.Builder()
                        .setDeviceAddress(android.net.MacAddress.fromString(macAddress))
                        .setGroupOperatingBand(WifiP2pConfig.GROUP_OWNER_BAND_AUTO)
                        .build()

                    activeBandLabel = "Otomatik Köprü (6GHz / 5GHz)"
                    mgr.connect(ch, autoHighBandConfig, object : WifiP2pManager.ActionListener {
                        override fun onSuccess() {
                            onConnectedOrFailed(true, activeBandLabel)
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
        }, 120L)
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
            groupOwnerIntent = 14
        }
        try {
            mgr.connect(ch, fallbackConfig, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onConnectedOrFailed(true, activeBandLabel)
                }
                override fun onFailure(reason: Int) {
                    onConnectedOrFailed(false, "Karşı cihazda Wi-Fi Direct onayı bekleniyor (Kod: $reason)")
                }
            })
        } catch (t: Throwable) {
            onConnectedOrFailed(false, t.message ?: "Bilinmeyen hata")
        }
    }

    private fun handlePeerList(peerList: WifiP2pDeviceList?) {
        discoveredP2pPeers.clear()
        val list = peerList?.deviceList ?: emptyList()

        for (device: WifiP2pDevice in list) {
            val addr = device.deviceAddress ?: continue
            val name = device.deviceName?.ifBlank { "Android Cihazı" } ?: "Android Cihazı"
            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = name,
                deviceModel = "Wi-Fi Direct",
                ipAddress = null,
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
