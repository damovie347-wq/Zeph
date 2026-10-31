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

class TransferForegroundService : Service() {

    companion object {
        const val CHANNEL_ID = "zeph_zero_copy_channel"
        const val NOTIFICATION_ID = 53317
        const val ACTION_START_ENGINE = "com.fasttransfer.p2p.action.START_ENGINE"
        const val ACTION_STOP_ENGINE = "com.fasttransfer.p2p.action.STOP_ENGINE"
        const val EXTRA_TITLE = "extra_title"
        const val EXTRA_SUBTITLE = "extra_subtitle"

        @Volatile
        var isRunning: Boolean = false
    }

    private var wakeLock: PowerManager.WakeLock? = null
    private var wifiLock: WifiManager.WifiLock? = null

    override fun onCreate() {
        super.onCreate()
        createNotificationChannelIfNeeded()
        acquireHardwareLocks()
        isRunning = true
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        Process.setThreadPriority(Process.THREAD_PRIORITY_URGENT_AUDIO)
        val action = intent?.action ?: ACTION_START_ENGINE

        if (action == ACTION_STOP_ENGINE) {
            stopAndCleanUp()
            return START_NOT_STICKY
        }

        val title = intent?.getStringExtra(EXTRA_TITLE) ?: "Zeph Yüksek Hızlı Aktarım"
        val subtitle = intent?.getStringExtra(EXTRA_SUBTITLE) ?: "Donanım hızlandırma aktif"
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
            isRunning = true
        } catch (_: Throwable) {
        }

        return START_NOT_STICKY
    }

    private fun stopAndCleanUp() {
        isRunning = false
        releaseHardwareLocks()
        try {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.N) {
                stopForeground(STOP_FOREGROUND_REMOVE)
            } else {
                @Suppress("DEPRECATION")
                stopForeground(true)
            }
        } catch (_: Throwable) {
        }
        stopSelf()
    }

    private fun acquireHardwareLocks() {
        try {
            val pm = applicationContext.getSystemService(Context.POWER_SERVICE) as? PowerManager
            if (pm != null && wakeLock == null) {
                wakeLock = pm.newWakeLock(
                    PowerManager.PARTIAL_WAKE_LOCK,
                    "Zeph::ZeroCopyWakeLock"
                ).apply {
                    setReferenceCounted(false)
                    acquire(30 * 60 * 1000L)
                }
            }
        } catch (_: Throwable) {
        }

        try {
            val wm = applicationContext.getSystemService(Context.WIFI_SERVICE) as? WifiManager
            if (wm != null && wifiLock == null) {
                val lockMode = CompatibilityHelper.getOptimalWifiLockMode()
                wifiLock = wm.createWifiLock(lockMode, "Zeph::ZeroCopyWifiLock").apply {
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
                "Zeph Aktarım Bildirimi",
                NotificationManager.IMPORTANCE_LOW
            ).apply {
                description = "Aktif veri aktarımı sırasında donanım uyanıklığını korur"
                setShowBadge(false)
            }
            mgr.createNotificationChannel(channel)
        }
    }

    private fun buildNotification(title: String, subtitle: String): Notification {
        val openIntent = Intent(this, MainActivity::class.java).apply {
            flags = Intent.FLAG_ACTIVITY_SINGLE_TOP or Intent.FLAG_ACTIVITY_CLEAR_TOP
        }
        val stopIntent = Intent(this, TransferForegroundService::class.java).apply {
            action = ACTION_STOP_ENGINE
        }

        val piFlags = if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        } else {
            PendingIntent.FLAG_UPDATE_CURRENT
        }
        val pendingOpen = PendingIntent.getActivity(this, 0, openIntent, piFlags)
        val pendingStop = PendingIntent.getService(this, 1, stopIntent, piFlags)

        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_wifi_receive)
            .setContentTitle(title)
            .setContentText(subtitle)
            .setContentIntent(pendingOpen)
            .setOngoing(false)
            .setAutoCancel(true)
            .setOnlyAlertOnce(true)
            .addAction(R.drawable.ic_close_x, "Bildirimi Kapat", pendingStop)
            .setPriority(NotificationCompat.PRIORITY_LOW)
            .build()
    }

    override fun onTaskRemoved(rootIntent: Intent?) {
        stopAndCleanUp()
        super.onTaskRemoved(rootIntent)
    }

    override fun onDestroy() {
        isRunning = false
        releaseHardwareLocks()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null
}
