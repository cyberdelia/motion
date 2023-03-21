package com.lapanthere.motion

import java.time.Instant

internal fun emptyBatch() = Batch()

internal class Batch(
    private val records: MutableList<Record> = mutableListOf(),
) : List<Record> by records {
    val byteSize: Int
        get() = records.sumOf { it.byteSize }

    val isExpired: Boolean
        get() = records.any { it.deadline.isBefore(Instant.now()) }

    fun add(record: Record) = records.add(record)

    fun completeExceptionally(t: Throwable): List<Record> {
        val (failures, queueable) = records.partition { it.isExpired }
        failures.forEach { it.completeExceptionally(t) }
        return queueable
    }
}
