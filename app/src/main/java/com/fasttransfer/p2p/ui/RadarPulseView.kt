package com.fasttransfer.p2p.ui

import android.animation.ValueAnimator
import android.content.Context
import android.graphics.Canvas
import android.graphics.Color
import android.graphics.Paint
import android.graphics.RectF
import android.util.AttributeSet
import android.view.View
import android.view.animation.LinearInterpolator
import kotlin.math.min

/**
 * Custom Canvas view replicating the exact segmented outer ring + solid inner circle
 * seen in the video at 00:00-00:03 and 00:38-00:39.
 */
class RadarPulseView @JvmOverloads constructor(
    context: Context,
    attrs: AttributeSet? = null,
    defStyleAttr: Int = 0
) : View(context, attrs, defStyleAttr) {

    private val innerPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        style = Paint.Style.FILL
        color = Color.parseColor("#6CD4FF")
    }

    private val segmentPaint = Paint(Paint.ANTI_ALIAS_FLAG).apply {
        style = Paint.Style.STROKE
        strokeCap = Paint.Cap.ROUND
        color = Color.parseColor("#6CD4FF")
    }

    private val arcRect = RectF()
    private var rotationDegrees = 0f
    private var pulseScale = 1.0f
    private var rotationAnimator: ValueAnimator? = null
    private var pulseAnimator: ValueAnimator? = null

    init {
        startAnimations()
    }

    fun setThemeColor(colorInt: Int) {
        innerPaint.color = colorInt
        segmentPaint.color = colorInt
        invalidate()
    }

    private fun startAnimations() {
        rotationAnimator = ValueAnimator.ofFloat(0f, 360f).apply {
            duration = 9000L
            repeatCount = ValueAnimator.INFINITE
            interpolator = LinearInterpolator()
            addUpdateListener { anim ->
                rotationDegrees = anim.animatedValue as Float
                invalidate()
            }
            start()
        }

        pulseAnimator = ValueAnimator.ofFloat(0.95f, 1.04f).apply {
            duration = 1400L
            repeatCount = ValueAnimator.INFINITE
            repeatMode = ValueAnimator.REVERSE
            addUpdateListener { anim ->
                pulseScale = anim.animatedValue as Float
                invalidate()
            }
            start()
        }
    }

    override fun onDraw(canvas: Canvas) {
        super.onDraw(canvas)
        val cx = width / 2f
        val cy = height / 2f
        val maxRadius = min(cx, cy) * 0.88f * pulseScale

        // Solid inner circle (matches video ratio)
        val innerRadius = maxRadius * 0.56f
        canvas.drawCircle(cx, cy, innerRadius, innerPaint)

        // Outer ring made of 10 rounded capsule segments (matches video 10-segment ring)
        val strokeW = maxRadius * 0.13f
        segmentPaint.strokeWidth = strokeW
        val ringRadius = maxRadius - strokeW / 2f
        arcRect.set(cx - ringRadius, cy - ringRadius, cx + ringRadius, cy + ringRadius)

        val segmentCount = 10
        val stepAngle = 360f / segmentCount
        val sweepAngle = 20f

        canvas.save()
        canvas.rotate(rotationDegrees, cx, cy)
        for (i in 0 until segmentCount) {
            val startAngle = i * stepAngle
            canvas.drawArc(arcRect, startAngle, sweepAngle, false, segmentPaint)
        }
        canvas.restore()
    }

    override fun onDetachedFromWindow() {
        rotationAnimator?.cancel()
        pulseAnimator?.cancel()
        super.onDetachedFromWindow()
    }
}
