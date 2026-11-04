package com.fasttransfer.p2p.ui

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.CheckBox
import android.widget.ImageView
import android.widget.TextView
import androidx.recyclerview.widget.RecyclerView
import com.fasttransfer.p2p.R
import com.fasttransfer.p2p.model.ApkAppItem
import com.fasttransfer.p2p.model.DeviceCategory
import com.fasttransfer.p2p.model.PeerDevice
import com.fasttransfer.p2p.util.StorageHelper

class NearbyDeviceAdapter(
    private val onPeerClicked: (PeerDevice) -> Unit
) : RecyclerView.Adapter<NearbyDeviceAdapter.PeerViewHolder>() {

    private val items = mutableListOf<PeerDevice>()

    fun submitList(newItems: List<PeerDevice>) {
        items.clear()
        items.addAll(newItems)
        notifyDataSetChanged()
    }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): PeerViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.item_nearby_device, parent, false)
        return PeerViewHolder(view)
    }

    override fun onBindViewHolder(holder: PeerViewHolder, position: Int) {
        val peer = items[position]
        holder.tvName.text = peer.alias
        holder.tvBandTag.text = peer.transport.label
        holder.tvAddress.text = peer.ipAddress ?: peer.macAddress ?: "Doğrudan Kanal"

        val iconRes = when (peer.category) {
            DeviceCategory.PHONE -> R.drawable.ic_device_phone
            DeviceCategory.LAPTOP -> R.drawable.ic_device_laptop
            DeviceCategory.TERMINAL -> R.drawable.ic_device_terminal
            DeviceCategory.SERVER -> R.drawable.ic_device_server
        }
        holder.ivIcon.setImageResource(iconRes)

        holder.itemView.setOnClickListener { onPeerClicked(peer) }
        holder.btnSend.setOnClickListener { onPeerClicked(peer) }
    }

    override fun getItemCount(): Int = items.size

    class PeerViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val ivIcon: ImageView = view.findViewById(R.id.ivPeerIcon)
        val tvName: TextView = view.findViewById(R.id.tvPeerName)
        val tvBandTag: TextView = view.findViewById(R.id.tvPeerBandTag)
        val tvAddress: TextView = view.findViewById(R.id.tvPeerAddress)
        val btnSend: TextView = view.findViewById(R.id.btnSendToPeer)
    }
}

class ApkListAdapter(
    private val onSingleAppSelected: (ApkAppItem) -> Unit,
    private val onMultiSelectionChanged: (List<ApkAppItem>) -> Unit
) : RecyclerView.Adapter<ApkListAdapter.ApkViewHolder>() {

    private val allApps = mutableListOf<ApkAppItem>()
    private val filteredApps = mutableListOf<ApkAppItem>()
    var isMultiSelectMode: Boolean = false
        set(value) {
            field = value
            notifyDataSetChanged()
        }

    fun setApps(apps: List<ApkAppItem>) {
        allApps.clear()
        allApps.addAll(apps)
        filteredApps.clear()
        filteredApps.addAll(apps)
        notifyDataSetChanged()
    }

    fun filter(query: String) {
        val q = query.trim().lowercase()
        filteredApps.clear()
        if (q.isEmpty()) {
            filteredApps.addAll(allApps)
        } else {
            for (app in allApps) {
                if (app.appName.lowercase().contains(q) || app.packageName.lowercase().contains(q)) {
                    filteredApps.add(app)
                }
            }
        }
        notifyDataSetChanged()
    }

    fun getFilteredCount(): Int = filteredApps.size

    fun getSelectedApps(): List<ApkAppItem> = allApps.filter { it.isSelected }

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ApkViewHolder {
        val view = LayoutInflater.from(parent.context)
            .inflate(R.layout.item_apk_app, parent, false)
        return ApkViewHolder(view)
    }

    override fun onBindViewHolder(holder: ApkViewHolder, position: Int) {
        val item = filteredApps[position]
        holder.tvLabel.text = item.appName
        holder.tvSizeAndVersion.text = "${StorageHelper.formatFileSize(item.sizeBytes)} • v${item.versionName}"
        holder.tvPackage.text = item.packageName
        if (item.icon != null) {
            holder.ivIcon.setImageDrawable(item.icon)
        } else {
            holder.ivIcon.setImageResource(R.drawable.ic_apps_grid)
        }

        holder.cbSelect.visibility = if (isMultiSelectMode) View.VISIBLE else View.GONE
        holder.cbSelect.isChecked = item.isSelected

        holder.itemView.setOnClickListener {
            if (isMultiSelectMode) {
                item.isSelected = !item.isSelected
                holder.cbSelect.isChecked = item.isSelected
                onMultiSelectionChanged(getSelectedApps())
            } else {
                onSingleAppSelected(item)
            }
        }
    }

    override fun getItemCount(): Int = filteredApps.size

    class ApkViewHolder(view: View) : RecyclerView.ViewHolder(view) {
        val ivIcon: ImageView = view.findViewById(R.id.ivApkIcon)
        val tvLabel: TextView = view.findViewById(R.id.tvApkLabel)
        val tvSizeAndVersion: TextView = view.findViewById(R.id.tvApkSizeAndVersion)
        val tvPackage: TextView = view.findViewById(R.id.tvApkPackage)
        val cbSelect: CheckBox = view.findViewById(R.id.cbApkSelect)
    }
}
