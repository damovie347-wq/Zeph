package com.fasttransfer.p2p.util

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.ConnectivityManager
import android.net.NetworkCapabilities
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Parcelable
import android.provider.Settings
import android.system.Os
import androidx.core.content.ContextCompat
import java.io.FileDescriptor
import java.lang.reflect.Method
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

object CompatibilityHelper {

    const val DEFAULT_DIRECT_PASSPHRASE: String = "zeph88889999"

    // Standard Linux kernel <linux/fadvise.h> constants (identical across ARM64/ARM/x86_64 Android kernels)
    private const val LINUX_POSIX_FADV_SEQUENTIAL: Int = 2
    private const val LINUX_POSIX_FADV_WILLNEED: Int = 3
    private const val LINUX_POSIX_FADV_DONTNEED: Int = 4

    private val PLACEHOLDER_NAMES = setOf(
        "kind carrot",
        "karo",
        "zeph cihazı",
        "android",
        "android cihazı",
        "unknown"
    )

    private val staSuspendedByMccGuard = AtomicBoolean(false)
    private val suspendedNetworkId = AtomicInteger(-1)

    // Cached reflective handle for `posix_fadvise` (hidden from public android.jar stubs in some SDK levels)
    private val posixFadviseMethod: Method? by lazy {
        try {
            Os::class.java.getMethod(
                "posix_fadvise",
                FileDescriptor::class.java,
                Long::class.javaPrimitiveType,
                Long::class.javaPrimitiveType,
                Int::class.javaPrimitiveType
            )
        } catch (_: Throwable) {
            null
        }
    }

    private fun invokePosixFadviseCompat(fd: FileDescriptor, offset: Long, length: Long, advice: Int) {
        try {
            val m = posixFadviseMethod
            if (m != null) {
                m.invoke(null, fd, offset, length, advice)
                return
            }
            val libcoreClass = Class.forName("libcore.io.Libcore")
            val osField = libcoreClass.getDeclaredField("os")
            osField.isAccessible = true
            val osInstance = osField.get(null) ?: return
            val fadvMethod = osInstance.javaClass.getMethod(
                "posix_fadvise",
                FileDescriptor::class.java,
                Long::class.javaPrimitiveType,
                Long::class.javaPrimitiveType,
                Int::class.javaPrimitiveType
            )
            fadvMethod.invoke(osInstance, fd, offset, length, advice)
        } catch (_: Throwable) {
        }
    }

    @Suppress("DEPRECATION")
    fun <T : Parcelable> getParcelableExtraCompat(intent: Intent, key: String, clazz: Class<T>): T? {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            intent.getParcelableExtra(key, clazz)
        } else {
            intent.getParcelableExtra(key)
        }
    }

    @Suppress("DEPRECATION")
    fun <T : Parcelable> getParcelableArrayListExtraCompat(
        intent: Intent,
        key: String,
        clazz: Class<T>
    ): ArrayList<T>? {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            intent.getParcelableArrayListExtra(key, clazz)
        } else {
            intent.getParcelableArrayListExtra(key)
        }
    }

    fun getRequiredRuntimePermissions(): Array<String> {
        val list = mutableListOf<String>()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            list.add(Manifest.permission.NEARBY_WIFI_DEVICES)
            list.add(Manifest.permission.ACCESS_FINE_LOCATION)
            list.add(Manifest.permission.ACCESS_COARSE_LOCATION)
            list.add(Manifest.permission.POST_NOTIFICATIONS)
            list.add(Manifest.permission.READ_MEDIA_IMAGES)
            list.add(Manifest.permission.READ_MEDIA_VIDEO)
            list.add(Manifest.permission.READ_MEDIA_AUDIO)
            list.add(Manifest.permission.READ_EXTERNAL_STORAGE)
            list.add(Manifest.permission.WRITE_EXTERNAL_STORAGE)
        } else {
            list.add(Manifest.permission.ACCESS_FINE_LOCATION)
            list.add(Manifest.permission.ACCESS_COARSE_LOCATION)
            list.add(Manifest.permission.READ_EXTERNAL_STORAGE)
            list.add(Manifest.permission.WRITE_EXTERNAL_STORAGE)
        }
        return list.toTypedArray()
    }

    fun hasAllRequiredPermissions(context: Context): Boolean {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            ContextCompat.checkSelfPermission(context, Manifest.permission.NEARBY_WIFI_DEVICES) ==
                PackageManager.PERMISSION_GRANTED
        } else {
            ContextCompat.checkSelfPermission(context, Manifest.permission.ACCESS_FINE_LOCATION) ==
                PackageManager.PERMISSION_GRANTED
        }
    }

    @Suppress("DEPRECATION")
    fun getOptimalWifiLockMode(): Int {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            WifiManager.WIFI_MODE_FULL_LOW_LATENCY
        } else {
            WifiManager.WIFI_MODE_FULL_HIGH_PERF
        }
    }

    @Suppress("DEPRECATION")
    fun suspendConflictingStaWifiForMcc(context: Context, p2pFreqMhz: Int): Boolean {
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager ?: return false
            val info = wm.connectionInfo ?: return false
            val staFreq = info.frequency
            val netId = info.networkId
            val ssid = info.ssid?.trim('"') ?: ""
            if (ssid.startsWith("DIRECT-")) return false

            val hasMccConflict = staFreq > 0 && p2pFreqMhz > 0 && kotlin.math.abs(staFreq - p2pFreqMhz) >= 20
            if (!hasMccConflict) return false

            if (netId >= 0) {
                suspendedNetworkId.set(netId)
                try {
                    wm.disableNetwork(netId)
                } catch (_: Throwable) {
                }
            }
            val disconnected = try {
                wm.disconnect()
            } catch (_: Throwable) {
                false
            }
            if (disconnected || netId >= 0) {
                staSuspendedByMccGuard.set(true)
                return true
            }
        } catch (_: Throwable) {
        }
        return false
    }

    @Suppress("DEPRECATION")
    fun restoreSuspendedStaWifi(context: Context) {
        if (!staSuspendedByMccGuard.compareAndSet(true, false)) return
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager ?: return
            val netId = suspendedNetworkId.getAndSet(-1)
            if (netId >= 0) {
                try {
                    wm.enableNetwork(netId, true)
                } catch (_: Throwable) {
                }
            }
            try {
                wm.reconnect()
            } catch (_: Throwable) {
            }
        } catch (_: Throwable) {
        }
    }

    fun applySequentialReadFadvise(fd: FileDescriptor, totalBytes: Long) {
        if (totalBytes <= 0L) return
        val prefetchLen = totalBytes.coerceAtMost(16L * 1024L * 1024L)
        invokePosixFadviseCompat(fd, 0L, totalBytes, LINUX_POSIX_FADV_SEQUENTIAL)
        invokePosixFadviseCompat(fd, 0L, prefetchLen, LINUX_POSIX_FADV_WILLNEED)
    }

    fun applySequentialWriteBehindFadvise(fd: FileDescriptor, writtenCursorBytes: Long) {
        val windowBytes = 32L * 1024L * 1024L
        if (writtenCursorBytes < windowBytes * 2) return
        val recentStart = (writtenCursorBytes - windowBytes * 2).coerceAtLeast(4L * 1024L * 1024L)
        invokePosixFadviseCompat(fd, recentStart, windowBytes, LINUX_POSIX_FADV_DONTNEED)
        if (writtenCursorBytes >= windowBytes * 3) {
            val olderStart = (writtenCursorBytes - windowBytes * 3).coerceAtLeast(4L * 1024L * 1024L)
            invokePosixFadviseCompat(fd, olderStart, windowBytes, LINUX_POSIX_FADV_DONTNEED)
        }
    }

    fun buildDeterministicDirectSsid(context: Context, alias: String): String {
        val tag = getShortDeviceTag(context)
        val normalized = alias.trim()
            .replace('ç', 'c').replace('Ç', 'C')
            .replace('ğ', 'g').replace('Ğ', 'G')
            .replace('ı', 'i').replace('İ', 'I')
            .replace('ö', 'o').replace('Ö', 'O')
            .replace('ş', 's').replace('Ş', 'S')
            .replace('ü', 'u').replace('Ü', 'U')
            .replace('+', 'P')
            .replace(' ', '_')
            .filter { it in 'a'..'z' || it in 'A'..'Z' || it in '0'..'9' || it == '_' }
            .take(16)
            .ifEmpty { "Android" }
        return "DIRECT-ZP-$tag-$normalized"
    }

    fun parseDirectZephSsid(ssid: String): Pair<String, String>? {
        val clean = ssid.trim().trim('"')
        if (!clean.startsWith("DIRECT-ZP-")) return null
        val rest = clean.removePrefix("DIRECT-ZP-")
        val dashIdx = rest.indexOf('-')
        return if (dashIdx > 0 && dashIdx < rest.length - 1) {
            val tag = rest.substring(0, dashIdx)
            val rawAlias = rest.substring(dashIdx + 1).replace('_', ' ').trim()
            tag to rawAlias.ifEmpty { "Zeph ($tag)" }
        } else {
            val tag = rest.take(2).ifEmpty { "01" }
            tag to "Zeph ($tag)"
        }
    }

    fun isNonPeerApplianceName(rawName: String, primaryDeviceType: String? = null): Boolean {
        val lower = rawName.trim().lowercase()
        if (lower.startsWith("[lg]") ||
            lower.contains("webos tv") ||
            lower.startsWith("[tv]") ||
            lower.startsWith("[samsung] tv") ||
            lower.contains("bravia") ||
            lower.contains("chromecast") ||
            lower.contains("firetv") ||
            lower.contains("roku") ||
            lower.contains("printer") ||
            lower.contains("direct-roku")
        ) {
            return true
        }
        if (!primaryDeviceType.isNullOrBlank()) {
            val cat = primaryDeviceType.substringBefore('-').toIntOrNull()
            if (cat == 3 || cat == 7) return true
        }
        return false
    }

    @Suppress("DEPRECATION")
    fun describeStaLinkToPeer(context: Context, peerIp: String): String? {
        try {
            val remote = parseIpv4Literal(peerIp) ?: return null
            if (peerIp.startsWith("192.168.49.")) return null
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager ?: return null
            var onStaSubnet = false
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                for (net in cm.allNetworks) {
                    val caps = cm.getNetworkCapabilities(net) ?: continue
                    if (!caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) continue
                    val lp = cm.getLinkProperties(net) ?: continue
                    val sameSubnet = lp.linkAddresses.any { la ->
                        val local = ipv4ToInt(la.address)
                        local != null && inSubnet(remote, local, la.prefixLength)
                    }
                    if (sameSubnet) {
                        onStaSubnet = true
                        break
                    }
                }
            }
            if (!onStaSubnet) return null
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager ?: return null
            val info = wm.connectionInfo ?: return null
            val tx = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) info.txLinkSpeedMbps else info.linkSpeed
            val rx = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) info.rxLinkSpeedMbps else info.linkSpeed
            val best = maxOf(tx, rx)
            if (best <= 0) return null
            val standard = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
                when (info.wifiStandard) {
                    4 -> "Wi-Fi 4 (11n)"
                    5 -> "Wi-Fi 5 (11ac)"
                    6 -> "Wi-Fi 6 (11ax)"
                    7 -> "11ad"
                    8 -> "Wi-Fi 7 (11be)"
                    else -> "Wi-Fi"
                }
            } else {
                "Wi-Fi"
            }
            val freq = info.frequency
            val lowMbs = best * 0.65 / 8.0
            val highMbs = best * 0.70 / 8.0
            return String.format(
                "Bağlantı: %s • %d MHz • Tx %d / Rx %d Mbps → pratik tavan ≈ %.0f–%.0f MB/s",
                standard, freq, tx, rx, lowMbs, highMbs
            )
        } catch (_: Throwable) {
            return null
        }
    }

    @Suppress("DEPRECATION")
    fun createHighPerfWifiLock(wifiManager: WifiManager, tag: String): WifiManager.WifiLock? {
        return try {
            wifiManager.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF, tag).apply {
                setReferenceCounted(false)
            }
        } catch (_: Throwable) {
            null
        }
    }

    fun getDeviceGoCapabilityScore(context: Context): Int {
        var score = Build.VERSION.SDK_INT
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null) {
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R && wm.is6GHzBandSupported) {
                    score += 300
                }
                if (wm.is5GHzBandSupported) {
                    score += 200
                }
            }
        } catch (_: Throwable) {
        }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            score += 150
        }
        return score
    }

    @Suppress("DEPRECATION")
    fun getActiveWifiFrequencyMhz(context: Context): Int {
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            val freq = wm?.connectionInfo?.frequency ?: 0
            if (freq > 0) return freq
        } catch (_: Throwable) {
        }
        return 0
    }

    fun isNonDfs5GhzP2pChannel(freqMhz: Int): Boolean {
        return freqMhz in 5180..5240 || freqMhz in 5745..5825
    }

    fun isPlaceholderDeviceName(name: String?): Boolean {
        if (name.isNullOrBlank()) return true
        return PLACEHOLDER_NAMES.contains(name.trim().lowercase())
    }

    @SuppressLint("HardwareIds")
    fun resolveUniqueDeviceName(context: Context): String {
        val mappedModel = mapRawModelToFriendlyName(Build.MANUFACTURER, Build.MODEL)

        val systemDeviceName = try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N_MR1) {
                Settings.Global.getString(context.contentResolver, Settings.Global.DEVICE_NAME)?.trim()
            } else {
                Settings.Secure.getString(context.contentResolver, "bluetooth_name")?.trim()
            }
        } catch (_: Throwable) {
            null
        }

        if (!systemDeviceName.isNullOrBlank() &&
            !isPlaceholderDeviceName(systemDeviceName) &&
            !systemDeviceName.startsWith("SM-", ignoreCase = true)
        ) {
            return systemDeviceName
        }

        if (mappedModel.isNotBlank()) {
            return mappedModel
        }

        val tag = getShortDeviceTag(context)
        return "Android $tag"
    }

    @SuppressLint("HardwareIds")
    fun getShortDeviceTag(context: Context): String {
        return try {
            val androidId = Settings.Secure.getString(context.contentResolver, Settings.Secure.ANDROID_ID) ?: ""
            val clean = androidId.filter { it.isLetterOrDigit() }
            if (clean.length >= 2) {
                clean.takeLast(2).uppercase()
            } else {
                "01"
            }
        } catch (_: Throwable) {
            "01"
        }
    }

    fun mapRawModelToFriendlyName(manufacturerRaw: String?, modelRaw: String?): String {
        val mfg = (manufacturerRaw ?: "").trim()
        val model = (modelRaw ?: "").trim()
        if (model.isEmpty()) return mfg.ifEmpty { "Android" }

        val u = model.uppercase()
        when {
            u.startsWith("23090RA98") -> return "Redmi Note 13 Pro+ 5G"
            u.startsWith("2312DRA50") -> return "Redmi Note 13 Pro 5G"
            u.startsWith("SM-G975") -> return "Galaxy S10+"
            u.startsWith("SM-G973") -> return "Galaxy S10"
            u.startsWith("SM-G970") -> return "Galaxy S10e"
            u.startsWith("SM-G977") -> return "Galaxy S10 5G"
            u.startsWith("SM-G770") -> return "Galaxy S10 Lite"
            u.startsWith("SM-X910") || u.startsWith("SM-X916") || u.startsWith("SM-X918") -> return "Galaxy Tab S9 Ultra"
            u.startsWith("SM-X810") || u.startsWith("SM-X816") || u.startsWith("SM-X818") -> return "Galaxy Tab S9+"
            u.startsWith("SM-X710") || u.startsWith("SM-X716") || u.startsWith("SM-X718") -> return "Galaxy Tab S9"
            u.startsWith("SM-X610") || u.startsWith("SM-X616") -> return "Galaxy Tab S9 FE+"
            u.startsWith("SM-X510") || u.startsWith("SM-X516") -> return "Galaxy Tab S9 FE"
            u.startsWith("SM-X900") || u.startsWith("SM-X906") -> return "Galaxy Tab S8 Ultra"
            u.startsWith("SM-X800") || u.startsWith("SM-X806") -> return "Galaxy Tab S8+"
            u.startsWith("SM-X700") || u.startsWith("SM-X706") -> return "Galaxy Tab S8"
            u.startsWith("SM-T970") || u.startsWith("SM-T976") -> return "Galaxy Tab S7+"
            u.startsWith("SM-T870") || u.startsWith("SM-T875") -> return "Galaxy Tab S7"
            u.startsWith("SM-S928") -> return "Galaxy S24 Ultra"
            u.startsWith("SM-S926") -> return "Galaxy S24+"
            u.startsWith("SM-S921") -> return "Galaxy S24"
            u.startsWith("SM-S918") -> return "Galaxy S23 Ultra"
            u.startsWith("SM-S916") -> return "Galaxy S23+"
            u.startsWith("SM-S911") -> return "Galaxy S23"
            u.startsWith("SM-S908") -> return "Galaxy S22 Ultra"
            u.startsWith("SM-S906") -> return "Galaxy S22+"
            u.startsWith("SM-S901") -> return "Galaxy S22"
            u.startsWith("SM-G998") -> return "Galaxy S21 Ultra"
            u.startsWith("SM-G996") -> return "Galaxy S21+"
            u.startsWith("SM-G991") -> return "Galaxy S21"
            u.startsWith("SM-G988") -> return "Galaxy S20 Ultra"
            u.startsWith("SM-G985") || u.startsWith("SM-G986") -> return "Galaxy S20+"
            u.startsWith("SM-G980") || u.startsWith("SM-G981") -> return "Galaxy S20"
            u.startsWith("SM-G780") || u.startsWith("SM-G781") -> return "Galaxy S20 FE"
        }

        if (model.startsWith(mfg, ignoreCase = true)) {
            return model
        }
        if (mfg.equals("samsung", ignoreCase = true) && u.startsWith("SM-")) {
            return "Samsung $model"
        }
        val cleanMfg = mfg.replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }
        return if (cleanMfg.isNotEmpty() && !model.contains(cleanMfg, ignoreCase = true)) {
            "$cleanMfg $model"
        } else {
            model
        }
    }

    fun isMobileHotspotActive(context: Context): Boolean {
        try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null) {
                val method = wm.javaClass.getDeclaredMethod("isWifiApEnabled")
                method.isAccessible = true
                val enabled = method.invoke(wm) as? Boolean
                if (enabled == true) return true
            }
        } catch (_: Throwable) {
        }

        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return false
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                val n = nif.name.lowercase()
                if (n.startsWith("ap") || n.startsWith("swlan") || n.startsWith("softap") || n.startsWith("wlan1")) {
                    for (addr in nif.inetAddresses) {
                        if (addr is Inet4Address && !addr.isLoopbackAddress) {
                            return true
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return false
    }

    private fun ipv4ToInt(addr: java.net.InetAddress?): Int? {
        val b = (addr as? Inet4Address)?.address ?: return null
        if (b.size != 4) return null
        return ((b[0].toInt() and 0xFF) shl 24) or ((b[1].toInt() and 0xFF) shl 16) or
            ((b[2].toInt() and 0xFF) shl 8) or (b[3].toInt() and 0xFF)
    }

    private fun parseIpv4Literal(s: String?): Int? {
        if (s.isNullOrBlank()) return null
        val parts = s.trim().split('.')
        if (parts.size != 4) return null
        var out = 0
        for (p in parts) {
            val v = p.toIntOrNull() ?: return null
            if (v < 0 || v > 255) return null
            out = (out shl 8) or v
        }
        return out
    }

    private fun inSubnet(ip: Int, netAddr: Int, prefixLen: Int): Boolean {
        if (prefixLen < 1 || prefixLen > 32) return false
        val mask = if (prefixLen == 32) -1 else (-1 shl (32 - prefixLen))
        return (ip and mask) == (netAddr and mask)
    }

    private fun isTunnelOrCellularInterfaceName(lowerName: String): Boolean {
        return lowerName.startsWith("rmnet") || lowerName.startsWith("ccmni") || lowerName.startsWith("pdp") ||
            lowerName.startsWith("tun") || lowerName.startsWith("tap") || lowerName.startsWith("dummy") ||
            lowerName.startsWith("lo") || lowerName.startsWith("v4-") || lowerName.startsWith("clat")
    }

    @Suppress("DEPRECATION")
    private fun isRemoteGatewayOfAnyWifiNetwork(context: Context, remote: Int): Boolean {
        try {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.M) return false
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager ?: return false
            for (net in cm.allNetworks) {
                val caps = cm.getNetworkCapabilities(net)
                if (caps != null && (caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) ||
                        caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN))
                ) {
                    continue
                }
                val lp = cm.getLinkProperties(net) ?: continue
                for (route in lp.routes) {
                    val gw = route.gateway ?: continue
                    if (ipv4ToInt(gw) == remote) return true
                }
            }
        } catch (_: Throwable) {
        }
        return false
    }

    @Suppress("DEPRECATION")
    private fun isRemoteOnLocalTetherInterface(context: Context, remote: Int): Boolean {
        try {
            val registered = HashSet<String>()
            var registryOk = false
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
                val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
                if (cm != null) {
                    for (net in cm.allNetworks) {
                        val ifName = cm.getLinkProperties(net)?.interfaceName
                        if (!ifName.isNullOrBlank()) registered.add(ifName)
                    }
                    registryOk = true
                }
            }
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return false
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                val name = nif.name ?: continue
                val lower = name.lowercase()
                if (isTunnelOrCellularInterfaceName(lower)) continue
                for (ia in nif.interfaceAddresses) {
                    val a = ia.address as? Inet4Address ?: continue
                    if (a.isLoopbackAddress) continue
                    val local = ipv4ToInt(a) ?: continue
                    if (!inSubnet(remote, local, ia.networkPrefixLength.toInt())) continue
                    val nameLooksLikeTether = lower.startsWith("p2p") || lower.startsWith("ap") ||
                        lower.startsWith("swlan") || lower.startsWith("softap") || lower.startsWith("wlan1") ||
                        lower.startsWith("rndis") || lower.startsWith("bt-pan") || lower.startsWith("bnep")
                    if (nameLooksLikeTether) return true
                    if (registryOk && !registered.contains(name)) return true
                }
            }
        } catch (_: Throwable) {
        }
        return false
    }

    fun isAlreadyDirectOrHotspotConnection(context: Context, remoteIp: String): Boolean {
        if (remoteIp.startsWith("192.168.49.")) return true
        val remote = parseIpv4Literal(remoteIp)

        if (remote != null && isRemoteGatewayOfAnyWifiNetwork(context, remote)) return true
        if (remote != null && isRemoteOnLocalTetherInterface(context, remote)) return true

        if (remoteIp.startsWith("192.168.43.")) return true
        if (isMobileHotspotActive(context)) return true

        try {
            val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M && cm != null) {
                val activeNet = cm.activeNetwork
                val caps = if (activeNet != null) cm.getNetworkCapabilities(activeNet) else null
                if (caps != null && caps.hasTransport(NetworkCapabilities.TRANSPORT_WIFI)) {
                    if (remoteIp.startsWith("10.") && !caps.hasCapability(NetworkCapabilities.NET_CAPABILITY_VALIDATED)) {
                        return true
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return false
    }

    fun isWifiRadioEnabled(context: Context): Boolean {
        return try {
            val wm = context.applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            wm?.isWifiEnabled == true
        } catch (_: Throwable) {
            true
        }
    }

    fun getLocalP2pInterfaceIp(): String? {
        return getLocalP2pInetAddress()?.hostAddress
    }

    fun getLocalP2pInetAddress(): Inet4Address? {
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return null
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        val ip = addr.hostAddress ?: continue
                        if (ip.startsWith("192.168.49.")) {
                            return addr
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return null
    }

    fun getAllActiveWifiAndP2pAddresses(): List<Inet4Address> {
        val result = mutableListOf<Inet4Address>()
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return result
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                val n = nif.name.lowercase()
                if (n.startsWith("rmnet") || n.startsWith("ccmni") || n.startsWith("pdp") ||
                    n.startsWith("tun") || n.startsWith("tap") || n.startsWith("dummy") || n.startsWith("lo")
                ) {
                    continue
                }
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        result.add(addr)
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return result
    }

    fun bindSocketToOptimalInterface(context: Context, socket: Socket, targetHostIp: String) {
        val target = parseIpv4Literal(targetHostIp) ?: return

        if (targetHostIp.startsWith("192.168.49.")) {
            try {
                val interfaces = NetworkInterface.getNetworkInterfaces()
                if (interfaces != null) {
                    for (nif in interfaces) {
                        if (!nif.isUp || nif.isLoopback) continue
                        for (addr in nif.inetAddresses) {
                            if (addr is Inet4Address && !addr.isLoopbackAddress) {
                                val localIp = addr.hostAddress ?: continue
                                if (localIp.startsWith("192.168.49.") && !socket.isBound) {
                                    socket.bind(InetSocketAddress(addr, 0))
                                    return
                                }
                            }
                        }
                    }
                }
            } catch (_: Throwable) {
            }
        }

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            try {
                val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
                @Suppress("DEPRECATION")
                val networks = cm?.allNetworks ?: emptyArray()
                for (net in networks) {
                    val caps = cm?.getNetworkCapabilities(net)
                    if (caps != null && (caps.hasTransport(NetworkCapabilities.TRANSPORT_CELLULAR) ||
                            caps.hasTransport(NetworkCapabilities.TRANSPORT_VPN))
                    ) {
                        continue
                    }
                    val linkProps = cm?.getLinkProperties(net) ?: continue
                    val onSubnet = linkProps.linkAddresses.any { la ->
                        val local = ipv4ToInt(la.address)
                        local != null && inSubnet(target, local, la.prefixLength)
                    }
                    val isGateway = linkProps.routes.any { r ->
                        val gw = r.gateway
                        gw != null && ipv4ToInt(gw) == target
                    }
                    if (onSubnet || isGateway) {
                        net.bindSocket(socket)
                        return
                    }
                }
            } catch (_: Throwable) {
            }
        }

        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                for (ia in nif.interfaceAddresses) {
                    val a = ia.address as? Inet4Address ?: continue
                    if (a.isLoopbackAddress) continue
                    val local = ipv4ToInt(a) ?: continue
                    if (inSubnet(target, local, ia.networkPrefixLength.toInt()) && !socket.isBound) {
                        socket.bind(InetSocketAddress(a, 0))
                        return
                    }
                }
            }
        } catch (_: Throwable) {
        }
    }

    fun startForegroundServiceCompat(context: Context, serviceIntent: Intent) {
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
                context.startForegroundService(serviceIntent)
            } else {
                context.startService(serviceIntent)
            }
        } catch (_: Throwable) {
        }
    }
}
