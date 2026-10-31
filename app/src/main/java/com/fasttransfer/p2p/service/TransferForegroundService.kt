package com.fasttransfer.p2p.service

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.net.wifi.WifiManager
import android.os.Build
import android.os.IBinder
import android.os.PowerManager
import android.os.Process
import androidx.core.app.NotificationCompat
import com.fasttransfer.p2p.MainActivity
import com.fasttransfer.p2p.R
import com.fasttransfer.p2p.util.CompatibilityHelper

/**
 * HARDWARE LOCK & HIGH-PRIORITY FOREGROUND SERVICE:
 * Keeps CPU awake (PARTIAL_WAKE_LOCK) and Wi-Fi chip in Ultra-Low-Latency / High-Perf mode
 * (WIFI_MODE_FULL_LOW_LATENCY on API 29+, WIFI_MODE_FULL_HIGH_PERF on API 21-28)
 * so screen-off or background transitions never throttle MB/s throughput.
 */
class TransferForegroundService : Service() {

    companion object {
        const val CHANNEL_ID = "fast_transfer_zero_copy_channel"
        const val NOTIFICATION_ID = 53317
        const val ACTION_START_ENGINE = "com.fasttransfer.p2p.action.START_ENGINE"
        const val ACTION_UPDATE_PROGRESS = "com.fasttransfer.p2p.action.UPDATE_PROGRESS"
        const val ACTION_STOP_ENGINE = "com.fasttransfer.p2p.action.STOP_ENGINE"
        const val EXTRA_TITLE = "extra_title"
        const val EXTRA_SUBTITLE = "extra_subtitle"
    }

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    override fun onCreate() {
        super.onCreate()
        createNotificationChannelIfNeeded()
        acquireHardwareLocks()
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
        val action = intent?.action ?: ACTION_START_ENGINE

        if (action == ACTION_STOP_ENGINE) {
            releaseHardwareLocks()
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                stopForeground(STOP_FOREGROUND_REMOVE)
            } else {
                @Suppress("DEPRECATION")
                stopForeground(true)
            }
            stopSelf()
            return START_NOT_STICKY
        }

        val title = intent?.getStringExtra(EXTRA_TITLE) ?: "FastTransfer Zero-Copy Motoru Aktif"
        val subtitle = intent?.getStringExtra(EXTRA_SUBTITLE) ?: "5 GHz / 2.4 GHz Maksimum Hız Hazır"
        val notification = buildNotification(title, subtitle)

        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                startForeground(
                    NOTIFICATION_ID,
                    notification,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
                )
            } else {
                startForeground(NOTIFICATION_ID, notification)
            }
        } catch (_: Throwable) {
        }

        return START_STICKY
    }

    private fun acquireHardwareLocks() {
        try {
            val pm = applicationContext.getSystemService(Context.POWER_SERVICE) as? PowerManager
            if (pm != null && wakeLock == null) {
                wakeLock = pm.newWakeLock(
                    PowerManager.PARTIAL_WAKE_LOCK,
                    "FastTransfer::ZeroCopyWakeLock"
                ).apply {
                    setReferenceCounted(false)
                    acquire(4 * 60 * 60 * 1000L) // Up to 4 hours continuous transfer
                }
            }
        } catch (_: Throwable) {
        }

        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null && wifiLock == null) {
                val lockMode = CompatibilityHelper.getOptimalWifiLockMode()
                wifiLock = wm.createWifiLock(lockMode, "FastTransfer::ZeroCopyWifiLock").apply {
                    setReferenceCounted(false)
                    acquire()
                }
            }
        } catch (_: Throwable) {
        }
    }

    private fun releaseHardwareLocks() {
        try {
            wakeLock?.let { if (it.isHeld) it.release() }
            wakeLock = null
        } catch (_: Throwable) {
        }
        try {
            wifiLock?.let { if (it.isHeld) it.release() }
            wifiLock = null
        } catch (_: Throwable) {
        }
    }

    private fun createNotificationChannelIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            val mgr = getSystemService(Context.NOTIFICATION_SERVICE) as? NotificationManager ?: return
            val channel = NotificationChannel(
                CHANNEL_ID,
                "FastTransfer Yüksek Hızlı Aktarım",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Zero-Copy Wi-Fi Direct ve LAN aktarım durumu"
                setShowBadge(false)
            }
            mgr.createNotificationChannel(channel)
        }
    }

    private fun buildNotification(title: String, subtitle: String): Notification {
        val openIntent = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val piFlags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        } else {
            PendingIntent.FLAG_UPDATE_CURRENT
        }
        val pendingIntent = PendingIntent.getActivity(this, 0, openIntent, piFlags)

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_wifi_receive)
            .setContentTitle(title)
            .setContentText(subtitle)
            .setContentIntent(pendingIntent)
            .setOngoing(true)
            .setOnlyAlertOnce(true)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    override fun onDestroy() {
        releaseHardwareLocks()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
