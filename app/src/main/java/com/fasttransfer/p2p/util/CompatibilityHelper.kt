package com.fasttransfer.p2p.util

import android.Manifest
import android.annotation.SuppressLint
import android.content.Context
import android.content.Intent
import android.content.pm.PackageManager
import android.net.ConnectivityManager
import android.net.Network
import android.net.NetworkCapabilities
import android.net.wifi.WifiManager
import android.os.Build
import android.os.Parcelable
import android.provider.Settings
import androidx.core.content.ContextCompat
import java.net.Inet4Address
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.Socket

object CompatibilityHelper {

    private val PLACEHOLDER_NAMES = setOf(
        "kind carrot",
        "karo",
        "zeph cihazı",
        "android",
        "android cihazı",
        "unknown"
    )

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
            list.add(Manifest.permission.POST_NOTIFICATIONS)
            list.add(Manifest.permission.READ_MEDIA_IMAGES)
            list.add(Manifest.permission.READ_MEDIA_VIDEO)
            list.add(Manifest.permission.READ_MEDIA_AUDIO)
        } else {
            list.add(Manifest.permission.ACCESS_FINE_LOCATION)
            list.add(Manifest.permission.ACCESS_COARSE_LOCATION)
            list.add(Manifest.permission.READ_EXTERNAL_STORAGE)
            if (Build.VERSION.SDK_INT <= Build.VERSION_CODES.P) {
                list.add(Manifest.permission.WRITE_EXTERNAL_STORAGE)
            }
        }
        return list.toTypedArray()
    }

    fun hasAllRequiredPermissions(context: Context): Boolean {
        return getRequiredRuntimePermissions().all { perm ->
            ContextCompat.checkSelfPermission(context, perm) == PackageManager.PERMISSION_GRANTED
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

    /**
     * Checks if legacy placeholder names ("Kind Carrot", "Karo", etc.) are stored so they can be
     * migrated automatically to the real device model name.
     */
    fun isPlaceholderDeviceName(name: String?): Boolean {
        if (name.isNullOrBlank()) return true
        return PLACEHOLDER_NAMES.contains(name.trim().lowercase())
    }

    /**
     * Resolves a human-readable, accurate device model/system name such as "Galaxy S10+",
     * "Galaxy Tab S9", "Pixel 8", etc. Never returns "Kind Carrot" or generic placeholders.
     */
    @SuppressLint("HardwareIds")
    fun resolveUniqueDeviceName(context: Context): String {
        val mappedModel = mapRawModelToFriendlyName(Build.MANUFACTURER, Build.MODEL)

        // Check user-configured system device name first (e.g. "Galaxy S10+" or "Galaxy Tab S9" on One UI)
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
            if (androidId.length >= 2) {
                androidId.takeLast(2).uppercase()
            } else {
                ((Build.MODEL?.hashCode() ?: 10) and 0xFF).toString(16).uppercase()
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
            // Galaxy S10 series
            u.startsWith("SM-G975") -> return "Galaxy S10+"
            u.startsWith("SM-G973") -> return "Galaxy S10"
            u.startsWith("SM-G970") -> return "Galaxy S10e"
            u.startsWith("SM-G977") -> return "Galaxy S10 5G"
            u.startsWith("SM-G770") -> return "Galaxy S10 Lite"
            // Galaxy Tab S9 series
            u.startsWith("SM-X910") || u.startsWith("SM-X916") || u.startsWith("SM-X918") -> return "Galaxy Tab S9 Ultra"
            u.startsWith("SM-X810") || u.startsWith("SM-X816") || u.startsWith("SM-X818") -> return "Galaxy Tab S9+"
            u.startsWith("SM-X710") || u.startsWith("SM-X716") || u.startsWith("SM-X718") -> return "Galaxy Tab S9"
            u.startsWith("SM-X610") || u.startsWith("SM-X616") -> return "Galaxy Tab S9 FE+"
            u.startsWith("SM-X510") || u.startsWith("SM-X516") -> return "Galaxy Tab S9 FE"
            // Galaxy Tab S8 / S7 series
            u.startsWith("SM-X900") || u.startsWith("SM-X906") -> return "Galaxy Tab S8 Ultra"
            u.startsWith("SM-X800") || u.startsWith("SM-X806") -> return "Galaxy Tab S8+"
            u.startsWith("SM-X700") || u.startsWith("SM-X706") -> return "Galaxy Tab S8"
            u.startsWith("SM-T970") || u.startsWith("SM-T976") -> return "Galaxy Tab S7+"
            u.startsWith("SM-T870") || u.startsWith("SM-T875") -> return "Galaxy Tab S7"
            // Galaxy S20 / S21 / S22 / S23 / S24 series
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

    /**
     * Detects whether Mobile Hotspot (SoftAP / Tethering) is currently active on this device.
     * When Mobile Hotspot is active, touching WifiP2pManager triggers annoying Samsung One UI
     * system popups ("Wi-Fi Direct kullanmak için Mobil Erişim Noktasını kapatın").
     * By detecting Hotspot state here, we completely silence all P2P calls while Hotspot is on!
     */
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
                for (addr in nif.inetAddresses) {
                    val ip = addr.hostAddress ?: continue
                    if (ip == "192.168.43.1" || ip.startsWith("192.168.43.") || ip.startsWith("192.168.194.")) {
                        if (n != "wlan0") return true
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

    /**
     * Finds our local IPv4 address on the P2P interface (192.168.49.x).
     */
    fun getLocalP2pInterfaceIp(): String? {
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return null
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        val ip = addr.hostAddress ?: continue
                        if (ip.startsWith("192.168.49.")) {
                            return ip
                        }
                    }
                }
            }
        } catch (_: Throwable) {
        }
        return null
    }

    /**
     * Binds an unconnected Socket directly to the local P2P / matching subnet interface
     * so Android's routing table NEVER sends 192.168.49.x traffic through the home router (wlan0)!
     */
    fun bindSocketToOptimalInterface(context: Context, socket: Socket, targetHostIp: String) {
        val targetPrefix = targetHostIp.substringBeforeLast('.', "")
        if (targetPrefix.isEmpty()) return

        // 1. Try binding via ConnectivityManager Network if Android tracks a matching Network object
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            try {
                val cm = context.getSystemService(Context.CONNECTIVITY_SERVICE) as? ConnectivityManager
                val networks = cm?.allNetworks ?: emptyArray()
                for (net in networks) {
                    val linkProps = cm?.getLinkProperties(net) ?: continue
                    val matchesSubnet = linkProps.linkAddresses.any { la ->
                        val hostAddr = la.address?.hostAddress ?: ""
                        hostAddr.startsWith("$targetPrefix.")
                    }
                    if (matchesSubnet) {
                        net.bindSocket(socket)
                        return
                    }
                }
            } catch (_: Throwable) {
            }
        }

        // 2. Bind directly to the matching NetworkInterface Inet4Address (works for all p2p-wlan0-x interfaces!)
        try {
            val interfaces = NetworkInterface.getNetworkInterfaces() ?: return
            for (nif in interfaces) {
                if (!nif.isUp || nif.isLoopback) continue
                for (addr in nif.inetAddresses) {
                    if (addr is Inet4Address && !addr.isLoopbackAddress) {
                        val localIp = addr.hostAddress ?: continue
                        if (localIp.startsWith("$targetPrefix.") && !socket.isBound) {
                            socket.bind(InetSocketAddress(addr, 0))
                            return
                        }
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
