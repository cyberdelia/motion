package com.lapanthere.motion

import io.github.bucket4j.Bandwidth
import io.github.bucket4j.Bucket
import io.github.bucket4j.Refill
import java.time.Duration

private const val RECORDS_PER_SECOND: Long = 1000
private const val BYTES_PER_SECOND: Long = 1_048_576

internal data class Shard(
    val name: String,
    val explicitHashKey: String,
) : Comparable<Shard> {
    private val records =
        Bucket.builder()
            .addLimit(Bandwidth.classic(RECORDS_PER_SECOND, Refill.intervally(RECORDS_PER_SECOND, Duration.ofSeconds(1))))
            .build()

    private val bytes =
        Bucket.builder()
            .addLimit(Bandwidth.classic(BYTES_PER_SECOND, Refill.intervally(BYTES_PER_SECOND, Duration.ofSeconds(1))))
            .build()

    private fun mayConsumeRecords(): Boolean =
        records.estimateAbilityToConsume(1)
            .canBeConsumed()

    private fun mayConsumeBytes(byteSize: Int): Boolean =
        bytes.estimateAbilityToConsume(byteSize.toLong())
            .canBeConsumed()

    val availableRecords: Int
        get() = records.availableTokens.toInt()

    val availableBytes: Int
        get() = bytes.availableTokens.toInt()

    val hasCapacity: Boolean
        get() = availableRecords > 0 && availableBytes > 0

    fun mayConsume(byteSize: Int): Boolean = mayConsumeRecords() && mayConsumeBytes(byteSize)

    fun consume(byteSize: Int): Boolean = records.tryConsume(1) && bytes.tryConsume(byteSize.toLong())

    override fun compareTo(other: Shard): Int = compareValuesBy(this, other, { it.name }, { it.explicitHashKey })
}
