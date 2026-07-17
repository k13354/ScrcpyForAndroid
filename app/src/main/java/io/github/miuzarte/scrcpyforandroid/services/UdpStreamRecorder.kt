package io.github.miuzarte.scrcpyforandroid.services

import android.util.Log
import kotlinx.coroutines.*
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.nio.ByteBuffer

/**
 * UDP 推流器：将 H.264 裸流封装为 MPEG-TS 并通过 UDP 发送到指定地址。
 *
 * 使用场景：配合 OBS 的 UDP 输入源（如 obs-udp-source 插件）实现低延迟直播。
 *
 * 实现要点：
 * - 使用 MPEG-TS (188字节) 封装 H.264 NALU
 * - PID = 0x100 (视频流)
 * - 自动插入 PAT/PMT 表（每 100 个视频包发送一次）
 * - 支持配置码率和目标地址
 */
class UdpStreamRecorder(
    private val targetHost: String,
    private val targetPort: Int,
    private val videoBitRate: Int = 8_000_000, // 默认 8Mbps
) {
    private val TAG = "UdpStreamRecorder"

    @Volatile
    private var running = false

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // MPEG-TS 常量
    companion object {
        private const val TS_PACKET_SIZE = 188
        private const val TS_HEADER_SIZE = 4
        private const val TS_PAYLOAD_SIZE = TS_PACKET_SIZE - TS_HEADER_SIZE
        private const val TS_ADAPTATION_FIELD_SIZE = 5 // 适配域大小（用于PCR）
        private const val TS_PAYLOAD_WITH_ADAPTATION = TS_PAYLOAD_SIZE - TS_ADAPTATION_FIELD_SIZE

        // PID 值
        private const val PAT_PID = 0x0000
        private const val PMT_PID = 0x0100
        private const val VIDEO_PID = 0x0101

        // Stream type for H.264
        private const val STREAM_TYPE_H264 = 0x1B

        // PAT/PMT 发送间隔（每 N 个视频包发送一次）
        private const val PSI_INTERVAL = 100

        private const val BUFFER_SIZE = 64 * 1024 // 64KB 发送缓冲区
    }

    private var socket: DatagramSocket? = null
    private var targetAddress: InetAddress? = null

    // 计数器
    private var videoPacketCounter = 0
    private var tsContinuityCounter = 0
    private var pcrCounter = 0
    private var totalBytesSent = 0L
    private var packetCount = 0L

    // PCR 时间戳（90kHz 时钟）
    private var pcrClock: Long = 0L
    private var lastPcrTimeNs = 0L

    @Volatile
    private var isStarted = false

    fun start() {
        if (running) {
            Log.w(TAG, "Already running")
            return
        }
        Log.i(TAG, "Starting UDP stream to $targetHost:$targetPort (bitrate=${videoBitRate / 1_000_000}Mbps)")

        running = true
        isStarted = true
        videoPacketCounter = 0
        tsContinuityCounter = 0
        totalBytesSent = 0L
        packetCount = 0L
        pcrClock = 0L
        lastPcrTimeNs = System.nanoTime()

        scope.launch {
            try {
                targetAddress = InetAddress.getByName(targetHost)
                socket = DatagramSocket().also {
                    it.sendBufferSize = BUFFER_SIZE
                    it.receiveBufferSize = BUFFER_SIZE
                }
                Log.i(TAG, "Socket opened, target=${targetAddress?.hostAddress}:$targetPort")

                // 初始发送 PAT/PMT
                sendPat()
                sendPmt()

            } catch (e: Exception) {
                Log.e(TAG, "Failed to start UDP stream", e)
                stop()
            }
        }
    }

    /**
     * 接收 H.264 裸流数据包并封装为 MPEG-TS 发送。
     *
     * @param data H.264 NALU 数据（包含起始码 0x00000001 或 0x000001）
     * @param ptsUs 显示时间戳（微秒）
     * @param isConfig 是否为 SPS/PPS 配置帧
     */
    fun feedPacket(data: ByteArray, ptsUs: Long, isConfig: Boolean = false) {
        if (!running || socket == null || targetAddress == null) return

        scope.launch {
            try {
                // 去除起始码（0x00000001 或 0x000001）
                val nalu = stripStartCode(data)
                if (nalu == null || nalu.isEmpty()) return@launch

                val naluType = getNaluType(nalu)
                val isKeyFrame = isConfig || naluType == 5 || naluType == 19 || naluType == 20

                // 将 NALU 分片为多个 TS 包
                splitAndSendTsPackets(nalu, ptsUs, isKeyFrame, isConfig)

            } catch (e: Exception) {
                Log.e(TAG, "Error feeding packet", e)
            }
        }
    }

    fun stop() {
        if (!running) return
        Log.i(TAG, "Stopping UDP stream. Total bytes sent: $totalBytesSent, packets: $packetCount")

        running = false
        isStarted = false

        scope.launch {
            try {
                socket?.close()
            } catch (_: Exception) {}
            socket = null
            targetAddress = null
        }
    }

    fun release() {
        stop()
        scope.cancel()
    }

    // ========== MPEG-TS 封装逻辑 ==========

    private fun splitAndSendTsPackets(nalu: ByteArray, ptsUs: Long, isKeyFrame: Boolean, isConfig: Boolean) {
        val totalSize = nalu.size
        var offset = 0
        val firstPacket = videoPacketCounter % PSI_INTERVAL == 0

        while (offset < totalSize) {
            val remaining = totalSize - offset
            val needAdaptation = (remaining > TS_PAYLOAD_WITH_ADAPTATION) ||
                    (offset == 0 && firstPacket) ||
                    (offset == 0 && videoPacketCounter % 25 == 0) // 每25个包插入PCR

            val tsPacket = ByteArray(TS_PACKET_SIZE)
            val buffer = ByteBuffer.wrap(tsPacket)

            // TS Header (4 bytes)
            buffer.put(0x47.toByte()) // Sync byte
            val flags = ((if (offset == 0) 0x40 else 0x00).toInt()) or
                    (VIDEO_PID shr 8)
            buffer.put(flags.toByte())
            buffer.put((VIDEO_PID and 0xFF).toByte())

            val hasAdaptation = needAdaptation && (offset == 0)
            val payloadSize = when {
                hasAdaptation -> TS_PAYLOAD_WITH_ADAPTATION
                else -> TS_PAYLOAD_SIZE
            }

            val scramblingControl = 0
            val priority = 0
            val continuityCounter = tsContinuityCounter and 0x0F
            tsContinuityCounter = (tsContinuityCounter + 1) and 0x0F

            val adaptationFieldControl = when {
                hasAdaptation -> 0x30 // Adaptation field + payload
                else -> 0x01 // Payload only
            }

            val tsc = ((scramblingControl shl 6) or
                    (priority shl 5) or
                    (adaptationFieldControl shl 4) or
                    continuityCounter).toByte()
            buffer.put(tsc)

            // Adaptation Field (if needed)
            if (hasAdaptation) {
                val afLength = (TS_ADAPTATION_FIELD_SIZE - 1).toByte()
                buffer.put(afLength)

                // Adaptation field flags: discontinuity indicator, random access indicator, PCR flag
                val afFlags = if (offset == 0) 0x50.toByte() else 0x10.toByte() // random_access + PCR
                buffer.put(afFlags)

                if (offset == 0 && (videoPacketCounter % 25 == 0 || firstPacket)) {
                    // PCR (6 bytes) - Program Clock Reference
                    val nowNs = System.nanoTime()
                    val elapsedNs = nowNs - lastPcrTimeNs
                    pcrClock += (elapsedNs / 11111L) // 90kHz
                    lastPcrTimeNs = nowNs

                    val pcrBase = pcrClock
                    val pcrExt = 0

                    buffer.put(((pcrBase shr 25) and 0xFF).toByte())
                    buffer.put(((pcrBase shr 17) and 0xFF).toByte())
                    buffer.put(((pcrBase shr 9) and 0xFF).toByte())
                    buffer.put(((pcrBase shr 1) and 0xFF).toByte())
                    buffer.put((((pcrBase and 1) shl 7) or 0x7E).toByte())
                    buffer.put((pcrExt shr 1).toByte())
                } else {
                    // Padding with 0xFF
                    for (i in 2 until TS_ADAPTATION_FIELD_SIZE) {
                        buffer.put(0xFF.toByte())
                    }
                }
            }

            // Payload
            val copySize = minOf(remaining, payloadSize)
            buffer.put(nalu, offset, copySize)
            offset += copySize

            // Send
            sendTsPacket(tsPacket)
            videoPacketCounter++
        }

        // Send PAT/PMT periodically
        if (videoPacketCounter % PSI_INTERVAL == 0) {
            sendPat()
            sendPmt()
        }
    }

    private fun sendTsPacket(data: ByteArray) {
        try {
            val packet = DatagramPacket(data, data.size, targetAddress!!, targetPort)
            socket?.send(packet)
            totalBytesSent += data.size
            packetCount++
        } catch (e: Exception) {
            Log.e(TAG, "Failed to send TS packet", e)
        }
    }

    // ========== PAT / PMT ==========

    private fun sendPat() {
        val pat = buildPat()
        val packets = encapsulatePsi(pat, PAT_PID)
        packets.forEach { sendTsPacket(it) }
        Log.d(TAG, "PAT sent")
    }

    private fun sendPmt() {
        val pmt = buildPmt()
        val packets = encapsulatePsi(pmt, PMT_PID)
        packets.forEach { sendTsPacket(it) }
        Log.d(TAG, "PMT sent")
    }

    private fun buildPat(): ByteArray {
        // PAT: table_id=0x00, section_length, transport_stream_id, version, section_number, last_section_number
        // Then N loops of (program_number, PID)
        val ba = ByteBuffer.allocate(1024)

        ba.put(0x00.toByte()) // table_id
        ba.put(0xB0.toByte()) // section_syntax_indicator=1, 0, reserved=11
        ba.put(0x00.toByte()) // section_length (filled later)
        ba.putShort(0x0001.toShort()) // transport_stream_id
        ba.put(0xC1.toByte()) // reserved=11, version=00000, current_next_indicator=1
        ba.put(0x00.toByte()) // section_number
        ba.put(0x00.toByte()) // last_section_number

        // Program loop
        ba.putShort(0x0001.toShort()) // program_number = 1
        ba.putShort((0xE000 or PMT_PID).toShort()) // reserved=111, program_map_PID

        // CRC32 placeholder
        val crcPos = ba.position()
        ba.putInt(0)

        // Fill section_length
        val sectionLength = ba.position() - 3
        ba.put(1, (0xB0 or ((sectionLength shr 8) and 0x0F)).toByte())
        ba.put(2, (sectionLength and 0xFF).toByte())

        // Calculate CRC32
        val crc = calculateCrc32(ba.array(), 1, sectionLength)
        ba.putInt(crcPos, crc)

        val result = ByteArray(ba.position())
        System.arraycopy(ba.array(), 0, result, 0, ba.position())
        return result
    }

    private fun buildPmt(): ByteArray {
        val ba = ByteBuffer.allocate(1024)

        ba.put(0x02.toByte()) // table_id (PMT)
        ba.put(0xB0.toByte())
        ba.put(0x00.toByte()) // section_length (filled later)
        ba.putShort(0x0001.toShort()) // program_number
        ba.put(0xC1.toByte()) // reserved, version, current_next
        ba.put(0x00.toByte()) // section_number
        ba.put(0x00.toByte()) // last_section_number

        // Program info
        ba.putShort(0xF000.toShort()) // program_info_length = 0

        // ES loop - H.264 video stream
        ba.put(STREAM_TYPE_H264.toByte()) // stream_type = H.264
        ba.putShort((0xE000 or VIDEO_PID).toShort()) // reserved, elementary_PID

        // ES descriptor
        val esInfoLength = 5 // H.264 video descriptor
        ba.putShort(esInfoLength.toShort())
        ba.put(0x02.toByte()) // video_stream_descriptor tag
        ba.put(0x05.toByte()) // length
        ba.put(0x00.toByte()) // reserved, num_frame_rates=0
        ba.put(0x00.toByte()) // MPEG-2 profile/level (not used for H.264)
        ba.put(0x00.toByte()) // reserved

        // CRC32 placeholder
        val crcPos = ba.position()
        ba.putInt(0)

        // Fill section_length
        val sectionLength = ba.position() - 3
        ba.put(1, (0xB0 or ((sectionLength shr 8) and 0x0F)).toByte())
        ba.put(2, (sectionLength and 0xFF).toByte())

        // Calculate CRC32
        val crc = calculateCrc32(ba.array(), 1, sectionLength)
        ba.putInt(crcPos, crc)

        val result = ByteArray(ba.position())
        System.arraycopy(ba.array(), 0, result, 0, ba.position())
        return result
    }

    private fun encapsulatePsi(psiData: ByteArray, pid: Int): List<ByteArray> {
        val packets = mutableListOf<ByteArray>()
        var offset = 0
        var counter = 0

        while (offset < psiData.size) {
            val tsPacket = ByteArray(TS_PACKET_SIZE)
            val buffer = ByteBuffer.wrap(tsPacket)

            // Fill with 0xFF first
            tsPacket.fill(0xFF.toByte())

            // TS Header
            buffer.position(0)
            buffer.put(0x47.toByte()) // Sync byte
            val flags = ((if (offset == 0) 0x40 else 0x00).toInt()) or (pid shr 8)
            buffer.put(flags.toByte())
            buffer.put((pid and 0xFF).toByte())

            val payloadSize = TS_PAYLOAD_SIZE
            val hasPayload = offset < psiData.size

            val adaptationFieldControl = if (hasPayload) 0x01 else 0x00
            val continuityCounter = counter and 0x0F
            counter = (counter + 1) and 0x0F

            val tsc = ((0 shl 6) or (0 shl 5) or (adaptationFieldControl shl 4) or continuityCounter).toByte()
            buffer.put(tsc)

            // Payload
            val copySize = minOf(psiData.size - offset, payloadSize)
            if (copySize > 0) {
                buffer.put(psiData, offset, copySize)
                offset += copySize
            }

            packets.add(tsPacket)
        }

        return packets
    }

    // ========== CRC32 for MPEG-TS ==========

    private fun calculateCrc32(data: ByteArray, offset: Int, length: Int): Int {
        var crc = 0xFFFFFFFF.toInt()
        for (i in offset until offset + length) {
            crc = crc xor (data[i].toInt() shl 24)
            for (j in 0 until 8) {
                crc = if (crc < 0) {
                    (crc shl 1) xor 0x04C11DB7
                } else {
                    crc shl 1
                }
            }
        }
        return crc
    }

    // ========== NALU 处理 ==========

    private fun stripStartCode(data: ByteArray): ByteArray? {
        if (data.size < 4) return null

        return when {
            data.size >= 4 && data[0] == 0x00.toByte() && data[1] == 0x00.toByte() &&
                    data[2] == 0x00.toByte() && data[3] == 0x01.toByte() -> {
                val result = ByteArray(data.size - 4)
                System.arraycopy(data, 4, result, 0, data.size - 4)
                result
            }
            data.size >= 3 && data[0] == 0x00.toByte() && data[1] == 0x00.toByte() &&
                    data[2] == 0x01.toByte() -> {
                val result = ByteArray(data.size - 3)
                System.arraycopy(data, 3, result, 0, data.size - 3)
                result
            }
            else -> {
                // No start code, return as-is (might be already stripped)
                data
            }
        }
    }

    private fun getNaluType(nalu: ByteArray): Int {
        if (nalu.isEmpty()) return 0
        return (nalu[0].toInt() and 0x1F)
    }

    val isRunning: Boolean get() = running
    val stats: String
        get() = "UDP packets=$packetCount bytes=$totalBytesSent target=$targetHost:$targetPort"
}
