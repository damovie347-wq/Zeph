package com.fasttransfer.p2p.network

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
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
import org.json.JSONObject
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.util.concurrent.ConcurrentHashMap

class LanDiscoveryManager(
    private val context: Context,
    private val getLocalAlias: () -> String,
    private val getLocalP2pGroup: () -> P2pGroupCredentials? = { null },
    private val transferPort: Int = 53317,
    private val discoveryUdpPort: Int = 53318,
    private val onLanPeersChanged: (List<PeerDevice>) -> Unit
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val peersByIp = ConcurrentHashMap<String, PeerDevice>()
    private var udpListenerJob: Job? = null
    private var udpBeaconJob: Job? = null
    private var multicastLock: WifiManager.MulticastLock? = null
    private var nsdManager: NsdManager? = null
    private var isNsdRegistered = false
    private var isNsdDiscovering = false

    private val SERVICE_TYPE = "_fasttransfer._tcp."

    fun getLocalDeviceModelName(): String {
        return CompatibilityHelper.mapRawModelToFriendlyName(Build.MANUFACTURER, Build.MODEL)
    }

    fun start() {
        acquireMulticastLock()
        startUdpListener()
        startUdpBeaconLoop()
        startNsdRegistrationAndDiscovery()
    }

    /**
     * Immediately removes stale 192.168.49.x peers when Wi-Fi Direct disconnects
     * so dead entries never linger on screen.
     */
    fun removePeersByPrefix(ipPrefix: String) {
        var changed = false
        val iterator = peersByIp.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (!entry.value.isFavorite && entry.key.startsWith(ipPrefix)) {
                iterator.remove()
                changed = true
            }
        }
        if (changed) {
            notifyPeersUpdated()
        }
    }

    private fun classifyTransportByIp(ip: String): ConnectionTransport {
        return if (ip.startsWith("192.168.49.") ||
            ip.startsWith("192.168.43.") ||
            CompatibilityHelper.isAlreadyDirectOrHotspotConnection(context, ip)
        ) {
            ConnectionTransport.WIFI_DIRECT_AUTO
        } else {
            ConnectionTransport.LAN_HOTSPOT
        }
    }

    private fun isWifiOrHotspotInterface(name: String): Boolean {
        val n = name.lowercase()
        if (n.startsWith("rmnet") || n.startsWith("ccmni") || n.startsWith("pdp") ||
            n.startsWith("tun") || n.startsWith("tap") || n.startsWith("dummy") || n.startsWith("lo")
        ) {
            return false
        }
        return true
    }

    private fun acquireMulticastLock() {
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null && multicastLock == null) {
                multicastLock = wm.createMulticastLock("ZephMulticastLock").apply {
                    setReferenceCounted(false)
                    acquire()
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun startUdpListener() {
        udpListenerJob?.cancel()
        udpListenerJob = scope.launch {
            while (isActive) {
                var socket: DatagramSocket? = null
                try {
                    socket = DatagramSocket(null).apply {
                        reuseAddress = true
                        broadcast = true
                        bind(InetSocketAddress(discoveryUdpPort))
                    }
                    val buffer = ByteArray(2048)
                    while (isActive) {
                        val packet = DatagramPacket(buffer, buffer.size)
                        socket.receive(packet)
                        val senderIp = packet.address?.hostAddress ?: continue
                        if (isOwnLocalIp(senderIp)) continue

                        val payload = String(packet.data, 0, packet.length, Charsets.UTF_8)
                        val json = JSONObject(payload)
                        val rawAlias = json.optString("alias", "").trim()
                        val model = json.optString("model", "Android").trim().ifEmpty { "Android" }
                        val port = json.optInt("port", transferPort)
                        val p2pSsid = json.optString("p2pSsid", "").takeIf { it.isNotBlank() }
                        val p2pPwd = json.optString("p2pPwd", "").takeIf { it.isNotBlank() }
                        val p2pFreq = json.optInt("p2pFreq", 0)

                        val cleanAlias = if (!CompatibilityHelper.isPlaceholderDeviceName(rawAlias)) {
                            rawAlias
                        } else if (!CompatibilityHelper.isPlaceholderDeviceName(model)) {
                            model
                        } else {
                            "Galaxy / Android (${senderIp.substringAfterLast('.')})"
                        }

                        val transport = classifyTransportByIp(senderIp)

                        val peer = PeerDevice(
                            id = "ip_$senderIp",
                            alias = cleanAlias,
                            deviceModel = model,
                            ipAddress = senderIp,
                            macAddress = null,
                            port = port,
                            category = DeviceCategory.PHONE,
                            transport = transport,
                            p2pSsid = p2pSsid,
                            p2pPassphrase = p2pPwd,
                            p2pFreqMhz = p2pFreq,
                            lastSeenMs = System.currentTimeMillis()
                        )
                        peersByIp[senderIp] = peer
                        notifyPeersUpdated()

                        if (json.optBoolean("ping", false)) {
                            sendUnicastAnnounce(senderIp)
                        }
                    }
                } catch (_: Throwable) {
                    delay(500L)
                } finally {
                    try {
                        socket?.close()
                    } catch (_: Throwable) {
                    }
                }
            }
        }
    }

    private fun startUdpBeaconLoop() {
        udpBeaconJob?.cancel()
        udpBeaconJob = scope.launch {
            while (isActive) {
                broadcastDiscoveryBeacon(isPing = true)
                pruneStalePeers()
                delay(1800L)
            }
        }
    }

    private fun buildBeaconJson(isPing: Boolean): JSONObject {
        val creds = getLocalP2pGroup()
        return JSONObject().apply {
            put("alias", getLocalAlias())
            put("model", getLocalDeviceModelName())
            put("port", transferPort)
            put("ping", isPing)
            if (creds != null) {
                put("p2pSsid", creds.ssid)
                put("p2pPwd", creds.passphrase)
                put("p2pFreq", creds.freqMhz)
            }
        }
    }

    /**
     * Broadcasts discovery beacons out of BOTH the default socket AND every bound local interface
     * (including p2p-wlan0-0 192.168.49.x and Hotspot swlan0/ap0) so P2P peers discover each other's
     * IP in < 20ms even when offline or when a Wi-Fi router is simultaneously connected.
     */
    fun broadcastDiscoveryBeacon(isPing: Boolean = true) {
        scope.launch {
            try {
                val bytes = buildBeaconJson(isPing).toString().toByteArray(Charsets.UTF_8)
                val localAddrs = CompatibilityHelper.getAllActiveWifiAndP2pAddresses()

                // 1. Send interface-bound packets so 192.168.49.x traffic is guaranteed to exit p2p-wlan0-0
                for (localAddr in localAddrs) {
                    val localIp = localAddr.hostAddress ?: continue
                    val subnetPrefix = localIp.substringBeforeLast('.', "")
                    if (subnetPrefix.isEmpty()) continue
                    try {
                        DatagramSocket(InetSocketAddress(localAddr, 0)).use { boundSock ->
                            boundSock.broadcast = true
                            val dests = mutableSetOf("$subnetPrefix.255", "$subnetPrefix.1", "255.255.255.255")
                            for (dst in dests) {
                                if (dst == localIp) continue
                                try {
                                    val pkt = DatagramPacket(
                                        bytes,
                                        bytes.size,
                                        InetAddress.getByName(dst),
                                        discoveryUdpPort
                                    )
                                    boundSock.send(pkt)
                                } catch (_: Throwable) {
                                }
                            }
                        }
                    } catch (_: Throwable) {
                    }
                }

                // 2. Also broadcast on default socket as fallback
                DatagramSocket().use { sock ->
                    sock.broadcast = true
                    val targets = mutableSetOf(
                        "255.255.255.255",
                        "192.168.49.1",
                        "192.168.49.255",
                        "192.168.43.1",
                        "192.168.43.255"
                    )
                    targets.addAll(getSubnetBroadcastAddresses())
                    for (addrStr in targets) {
                        try {
                            val packet = DatagramPacket(
                                bytes,
                                bytes.size,
                                InetAddress.getByName(addrStr),
                                discoveryUdpPort
                            )
                            sock.send(packet)
                        } catch (_: Throwable) {
                        }
                    }
                }
            } catch (_: Throwable) {
            }
        }
    }

    fun sendUnicastAnnounce(targetIp: String) {
        scope.launch {
            try {
                val bytes = buildBeaconJson(isPing = false).toString().toByteArray(Charsets.UTF_8)
                val targetPrefix = targetIp.substringBeforeLast('.', "")
                val matchingLocal = CompatibilityHelper.getAllActiveWifiAndP2pAddresses().firstOrNull {
                    it.hostAddress?.startsWith("$targetPrefix.") == true
                }
                if (matchingLocal != null) {
                    try {
                        DatagramSocket(InetSocketAddress(matchingLocal, 0)).use { boundSock ->
                            val packet = DatagramPacket(
                                bytes,
                                bytes.size,
                                InetAddress.getByName(targetIp),
                                discoveryUdpPort
                            )
                            boundSock.send(packet)
                            return@launch
                        }
                    } catch (_: Throwable) {
                    }
                }

                DatagramSocket().use { sock ->
                    val packet = DatagramPacket(
                        bytes,
                        bytes.size,
                        InetAddress.getByName(targetIp),
                        discoveryUdpPort
                    )
                    sock.send(packet)
                }
            } catch (_: Throwable) {
            }
        }
    }

    fun addManualFavoritePeer(
        ipAddress: String,
        alias: String = "Cihaz ($ipAddress)",
        isP2pBridge: Boolean = false
    ) {
        val cleanIp = ipAddress.trim()
        if (cleanIp.isEmpty() || isOwnLocalIp(cleanIp)) return
        val transport = if (isP2pBridge) ConnectionTransport.WIFI_DIRECT_AUTO else classifyTransportByIp(cleanIp)
        peersByIp[cleanIp] = PeerDevice(
            id = "fav_$cleanIp",
            alias = alias,
            deviceModel = "Android",
            ipAddress = cleanIp,
            macAddress = null,
            port = transferPort,
            category = DeviceCategory.PHONE,
            transport = transport,
            isFavorite = true,
            lastSeenMs = System.currentTimeMillis() + 3_600_000L
        )
        notifyPeersUpdated()
        sendUnicastAnnounce(cleanIp)
    }

    private fun startNsdRegistrationAndDiscovery() {
        try {
            val mgr = context.getSystemService(Context.NSD_SERVICE) as? NsdManager ?: return
            nsdManager = mgr
            val serviceInfo = NsdServiceInfo().apply {
                serviceName = "Zeph_${getLocalAlias().replace(" ", "_")}"
                serviceType = SERVICE_TYPE
                port = transferPort
            }
            mgr.registerService(serviceInfo, NsdManager.PROTOCOL_DNS_SD, nsdRegListener)
            mgr.discoverServices(SERVICE_TYPE, NsdManager.PROTOCOL_DNS_SD, nsdDiscoveryListener)
        } catch (_: Throwable) {
        }
    }

    private val nsdRegListener = object : NsdManager.RegistrationListener {
        override fun onServiceRegistered(serviceInfo: NsdServiceInfo?) {
            isNsdRegistered = true
        }
        override fun onRegistrationFailed(serviceInfo: NsdServiceInfo?, errorCode: Int) {}
        override fun onServiceUnregistered(serviceInfo: NsdServiceInfo?) {
            isNsdRegistered = false
        }
        override fun onUnregistrationFailed(serviceInfo: NsdServiceInfo?, errorCode: Int) {}
    }

    private val nsdDiscoveryListener = object : NsdManager.DiscoveryListener {
        override fun onDiscoveryStarted(regType: String?) {
            isNsdDiscovering = true
        }
        override fun onServiceFound(service: NsdServiceInfo?) {
            val s = service ?: return
            if (!s.serviceType.contains("_fasttransfer")) return
            try {
                @Suppress("DEPRECATION")
                nsdManager?.resolveService(s, object : NsdManager.ResolveListener {
                    override fun onResolveFailed(serviceInfo: NsdServiceInfo?, errorCode: Int) {}
                    override fun onServiceResolved(resolved: NsdServiceInfo?) {
                        val info = resolved ?: return
                        val hostIp = info.host?.hostAddress ?: return
                        if (isOwnLocalIp(hostIp)) return
                        if (peersByIp.containsKey(hostIp)) return
                        val rawName = info.serviceName
                            .removePrefix("Zeph_")
                            .removePrefix("FastTransfer_")
                            .replace("_", " ")
                            .trim()
                        val displayAlias = if (!CompatibilityHelper.isPlaceholderDeviceName(rawName)) {
                            rawName
                        } else {
                            "Cihaz ($hostIp)"
                        }
                        peersByIp[hostIp] = PeerDevice(
                            id = "ip_$hostIp",
                            alias = displayAlias,
                            deviceModel = "Android",
                            ipAddress = hostIp,
                            macAddress = null,
                            port = info.port.takeIf { it > 0 } ?: transferPort,
                            category = DeviceCategory.PHONE,
                            transport = classifyTransportByIp(hostIp),
                            lastSeenMs = System.currentTimeMillis()
                        )
                        notifyPeersUpdated()
                    }
                })
            } catch (_: Throwable) {
            }
        }
        override fun onServiceLost(service: NsdServiceInfo?) {}
        override fun onDiscoveryStopped(serviceType: String?) {
            isNsdDiscovering = false
        }
        override fun onStartDiscoveryFailed(serviceType: String?, errorCode: Int) {}
        override fun onStopDiscoveryFailed(serviceType: String?, errorCode: Int) {}
    }

    private fun pruneStalePeers() {
        val now = System.currentTimeMillis()
        var changed = false
        val p2pInterfaceActive = CompatibilityHelper.getLocalP2pInterfaceIp() != null
        val iterator = peersByIp.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (entry.value.isFavorite) continue
            if (entry.key.startsWith("192.168.49.") && !p2pInterfaceActive) {
                iterator.remove()
                changed = true
            } else if (now - entry.value.lastSeenMs > 8_000L) {
                iterator.remove()
                changed = true
            }
        }
        if (changed) {
            notifyPeersUpdated()
        }
    }

    private fun notifyPeersUpdated() {
        val rawList = peersByIp.values.toList()
        val nameCounts = rawList.groupingBy { it.alias.lowercase() }.eachCount()
        val disambiguated = rawList.map { peer ->
            if ((nameCounts[peer.alias.lowercase()] ?: 0) > 1 && !peer.ipAddress.isNullOrBlank()) {
                val suffix = peer.ipAddress.substringAfterLast('.')
                peer.copy(alias = "${peer.alias} (#$suffix)")
            } else {
                peer
            }
        }
        onLanPeersChanged(
            disambiguated.sortedWith(
                compareBy<PeerDevice> { it.transport.priority }.thenBy { it.alias }
            )
        )
    }

    fun getLocalIpAddresses(): List<String> {
        val ips = mutableListOf<String>()
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return ips
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback || !isWifiOrHotspotInterface(nif.name)) continue
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        addr.hostAddress?.let { ips.add(it) }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return ips.sortedBy { if (it.startsWith("192.168.49.")) 0 else 1 }
    }

    private fun isOwnLocalIp(ip: String): Boolean {
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return false
            for (nif in interfaces) {
                for (addr in nif.inetAddresses) {
                    if (addr.hostAddress == ip) return true
                }
            }
        } catch (_: Throwable) {
        }
        return false
    }

    private fun getSubnetBroadcastAddresses(): List<String> {
        val list = mutableListOf<String>()
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return list
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback || !isWifiOrHotspotInterface(nif.name)) continue
                for (ia in nif.interfaceAddresses) {
                    val bcast = ia.broadcast
                    if (bcast is Inet4Address) {
                        bcast.hostAddress?.let { list.add(it) }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return list
    }

    fun stop() {
        udpListenerJob?.cancel()
        udpBeaconJob?.cancel()
        try {
            if (isNsdDiscovering) nsdManager?.stopServiceDiscovery(nsdDiscoveryListener)
        } catch (_: Throwable) {
        }
        try {
            if (isNsdRegistered) nsdManager?.unregisterService(nsdRegListener)
        } catch (_: Throwable) {
        }
        try {
            multicastLock?.let { if (it.isHeld) it.release() }
        } catch (_: Throwable) {
        }
    }
}
