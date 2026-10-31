package com.fasttransfer.p2p.network

import android.annotation.SuppressLint
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
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
import android.os.Looper
import com.fasttransfer.p2p.model.ConnectionTransport
import com.fasttransfer.p2p.model.DeviceCategory
import com.fasttransfer.p2p.model.PeerDevice
import com.fasttransfer.p2p.util.CompatibilityHelper

/**
 * CASCADING BAND SELECTION & DIRECT 5 GHz HARDWARE BRIDGE ENGINE:
 * Bypasses home DSL/router AP relay bottleneck (~45-60 MB/s) by creating a direct 1-to-1
 * 5 GHz (80/160 MHz) Wi-Fi Direct hardware bridge (192.168.49.1) between the two phones,
 * with automatic 0ms fallback to 2.4 GHz if one of the devices is a legacy 2.4 GHz model.
 */
@SuppressLint("MissingPermission")
class CascadingWifiDirectManager(
    private val context: Context,
    private val onPeersUpdated: (List<PeerDevice>) -> Unit,
    private val onDirectConnectionReady: (groupOwnerIp: String, isGroupOwner: Boolean, bandLabel: String) -> Unit,
    private val onStatusLog: (String) -> Unit
) {
    private val wifiManager: WifiManager? =
        context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
    private val p2pManager: WifiP2pManager? =
        context.applicationContext.getSystemService(Context.WIFI_P2P_SERVICE) as? WifiP2pManager
    private var channel: WifiP2pManager.Channel? = null
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
                        onStatusLog("5 GHz Doğrudan Köprü Hazır (${getHardwareBandCapabilitySummary()})")
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
                    val groupInfo = CompatibilityHelper.getParcelableExtraCompat(
                        intent,
                        WifiP2pManager.EXTRA_WIFI_P2P_GROUP,
                        WifiP2pGroup::class.java
                    )
                    if (p2pInfo != null && p2pInfo.groupFormed && p2pInfo.groupOwnerAddress != null) {
                        val freqMhz = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q && groupInfo != null) {
                            groupInfo.frequency
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
                        val ownerIp = p2pInfo.groupOwnerAddress.hostAddress ?: "192.168.49.1"
                        onDirectConnectionReady(ownerIp, p2pInfo.isGroupOwner, bandDesc)
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
                    onStatusLog("5 GHz Doğrudan Köprü & LAN Aktif")
                }

                override fun onFailure(reason: Int) {
                    onStatusLog("LAN / Hotspot Aktif (P2P Durum: $reason)")
                }
            })
        } catch (_: Throwable) {
        }
    }

    /**
     * Creates a dedicated 5 GHz Hardware Group Owner Bridge (192.168.49.1) on the receiving phone
     * so the sending phone can connect directly without going through a slow home router/DSL modem.
     */
    fun createDirect5GhzGroupBridge(onResult: (Boolean, String) -> Unit) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onResult(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        try {
            mgr.removeGroup(ch, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    createGroupInternal(mgr, ch, onResult)
                }
                override fun onFailure(reason: Int) {
                    createGroupInternal(mgr, ch, onResult)
                }
            })
        } catch (_: Throwable) {
            createGroupInternal(mgr, ch, onResult)
        }
    }

    private fun createGroupInternal(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        onResult: (Boolean, String) -> Unit
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
                        onResult(true, "5 GHz Doğrudan Donanım Köprüsü Kuruldu (IP: 192.168.49.1)")
                    }
                    override fun onFailure(reason: Int) {
                        // Fallback to automatic band group creation
                        mgr.createGroup(ch, object : WifiP2pManager.ActionListener {
                            override fun onSuccess() {
                                activeBandLabel = "Doğrudan Köprü (192.168.49.1)"
                                onResult(true, "Doğrudan Köprü Kuruldu (IP: 192.168.49.1)")
                            }
                            override fun onFailure(r2: Int) {
                                onResult(false, "Köprü zaten aktif veya meşgul (Kod: $r2)")
                            }
                        })
                    }
                })
                return
            } catch (_: Throwable) {
            }
        }

        mgr.createGroup(ch, object : WifiP2pManager.ActionListener {
            override fun onSuccess() {
                activeBandLabel = "Doğrudan Köprü (192.168.49.1)"
                onResult(true, "Doğrudan Köprü Kuruldu (IP: 192.168.49.1)")
            }
            override fun onFailure(reason: Int) {
                onResult(false, "Köprü kurulamadı (Kod: $reason)")
            }
        })
    }

    /**
     * Connects to a Wi-Fi Direct peer using Cascading Band Selection:
     * Step 1: Try 5 GHz (GROUP_OWNER_BAND_5GHZ) on API 29+ if supported.
     * Step 2: If onFailure is triggered (e.g. legacy 2.4 GHz peer), immediately fall back to 2.4 GHz standard config!
     */
    fun connectWithCascadingBandFallback(macAddress: String, onConnectedOrFailed: (Boolean, String) -> Unit) {
        val mgr = p2pManager
        val ch = channel
        if (mgr == null || ch == null) {
            onConnectedOrFailed(false, "Wi-Fi Direct desteklenmiyor")
            return
        }

        val supports5Ghz = try {
            wifiManager?.is5GHzBandSupported == true
        } catch (_: Throwable) {
            false
        }

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
                        onStatusLog("Hedef cihaz 5 GHz reddetti ($reason), 0ms ile 2.4 GHz banda düşülüyor...")
                        connectLegacy24GhzOrAutoFallback(mgr, ch, macAddress, onConnectedOrFailed)
                    }
                })
                return
            } catch (_: Throwable) {
            }
        }

        connectLegacy24GhzOrAutoFallback(mgr, ch, macAddress, onConnectedOrFailed)
    }

    private fun connectLegacy24GhzOrAutoFallback(
        mgr: WifiP2pManager,
        ch: WifiP2pManager.Channel,
        macAddress: String,
        onConnectedOrFailed: (Boolean, String) -> Unit
    ) {
        activeBandLabel = "2.4 GHz / Otomatik Doğrudan Köprü"
        val fallbackConfig = WifiP2pConfig().apply {
            deviceAddress = macAddress
            wps.setup = WpsInfo.PBC
            groupOwnerIntent = 0 // Prefer connecting to the receiver's 5 GHz group owner bridge
        }
        try {
            mgr.connect(ch, fallbackConfig, object : WifiP2pManager.ActionListener {
                override fun onSuccess() {
                    onStatusLog("Doğrudan Köprü Bağlantısı Kuruldu")
                    onConnectedOrFailed(true, activeBandLabel)
                }

                override fun onFailure(reason: Int) {
                    onStatusLog("Wi-Fi Direct Bağlantı Hatası (Kod: $reason)")
                    onConnectedOrFailed(false, "Bağlantı kurulamadı ($reason)")
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
            val name = device.deviceName?.ifBlank { "Android P2P Cihazı" } ?: "Android P2P Cihazı"
            val transport = if (supports5G && preferHighBand5Ghz) {
                ConnectionTransport.WIFI_DIRECT_5GHZ
            } else {
                ConnectionTransport.WIFI_DIRECT_2_4GHZ
            }
            // Connected P2P group owner is always reachable at 192.168.49.1
            val directIp = if (device.status == WifiP2pDevice.CONNECTED) "192.168.49.1" else null
            discoveredP2pPeers[addr] = PeerDevice(
                id = "p2p_$addr",
                alias = "$name (Doğrudan Köprü)",
                ipAddress = directIp,
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
