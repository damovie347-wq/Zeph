package com.fasttransfer.p2p.network

import android.content.Context
import android.net.nsd.NsdManager
import android.net.nsd.NsdServiceInfo
import android.net.wifi.WifiManager
import com.fasttransfer.p2p.model.ConnectionTransport
import com.fasttransfer.p2p.model.DeviceCategory
import com.fasttransfer.p2p.model.PeerDevice
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

    fun start() {
        acquireMulticastLock()
        startUdpListener()
        startUdpBeaconLoop()
        startNsdRegistrationAndDiscovery()
    }

    private fun classifyTransportByIp(ip: String): ConnectionTransport {
        return if (ip.startsWith("192.168.49.") || ip.startsWith("192.168.43.")) {
            // 192.168.49.x is Android Wi-Fi Direct 5GHz/2.4GHz hardware bridge; 192.168.43.x is Direct Hotspot
            ConnectionTransport.WIFI_DIRECT_5GHZ
        } else {
            ConnectionTransport.LAN_HOTSPOT
        }
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
                    val alias = json.optString("alias", "Zeph Cihazı")
                    val port = json.optInt("port", transferPort)
                    val transport = classifyTransportByIp(senderIp)

                    val peer = PeerDevice(
                        id = "ip_$senderIp",
                        alias = alias,
                        ipAddress = senderIp,
                        macAddress = null,
                        port = port,
                        category = DeviceCategory.PHONE,
                        transport = transport,
                        lastSeenMs = System.currentTimeMillis()
                    )
                    peersByIp[senderIp] = peer
                    notifyPeersUpdated()

                    if (json.optBoolean("ping", false)) {
                        sendUnicastAnnounce(senderIp)
                    }
                }
            } catch (_: Throwable) {
            } finally {
                try {
                    socket?.close()
                } catch (_: Throwable) {
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
                delay(2200L)
            }
        }
    }

    fun broadcastDiscoveryBeacon(isPing: Boolean = true) {
        scope.launch {
            try {
                val json = JSONObject().apply {
                    put("alias", getLocalAlias())
                    put("port", transferPort)
                    put("ping", isPing)
                }
                val bytes = json.toString().toByteArray(Charsets.UTF_8)
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
                val json = JSONObject().apply {
                    put("alias", getLocalAlias())
                    put("port", transferPort)
                    put("ping", false)
                }
                val bytes = json.toString().toByteArray(Charsets.UTF_8)
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
        alias: String = "Doğrudan Cihaz ($ipAddress)",
        isP2pBridge: Boolean = false
    ) {
        val cleanIp = ipAddress.trim()
        if (cleanIp.isEmpty() || isOwnLocalIp(cleanIp)) return
        val transport = if (isP2pBridge) ConnectionTransport.WIFI_DIRECT_5GHZ else classifyTransportByIp(cleanIp)
        peersByIp[cleanIp] = PeerDevice(
            id = "fav_$cleanIp",
            alias = alias,
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
                        val rawName = info.serviceName
                            .removePrefix("Zeph_")
                            .removePrefix("FastTransfer_")
                            .replace("_", " ")
                        peersByIp[hostIp] = PeerDevice(
                            id = "ip_$hostIp",
                            alias = rawName.ifBlank { "Cihaz ($hostIp)" },
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
        val iterator = peersByIp.entries.iterator()
        while (iterator.hasNext()) {
            val entry = iterator.next()
            if (!entry.value.isFavorite && now - entry.value.lastSeenMs > 16_000L) {
                iterator.remove()
                changed = true
            }
        }
        if (changed) {
            notifyPeersUpdated()
        }
    }

    private fun notifyPeersUpdated() {
        onLanPeersChanged(
            peersByIp.values.sortedWith(
                compareBy<PeerDevice> { it.transport.priority }.thenBy { it.alias }
            )
        )
    }

    fun getLocalIpAddresses(): List<String> {
        val ips = mutableListOf<String>()
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return ips
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        addr.hostAddress?.let { ips.add(it) }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        // Sort 192.168.49.x (Wi-Fi Direct 5GHz bridge) first
        return ips.sortedBy { if (it.startsWith("192.168.49.")) 0 else 1 }
    }

    private fun isOwnLocalIp(ip: String): Boolean {
        return getLocalIpAddresses().contains(ip)
    }

    private fun getSubnetBroadcastAddresses(): List<String> {
        val list = mutableListOf<String>()
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return list
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
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
