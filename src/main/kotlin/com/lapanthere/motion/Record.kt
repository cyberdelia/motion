package com.lapanthere.motion

import java.time.Instant
import java.util.concurrent.CompletableFuture

internal data class Record(
    val raw: ByteArray,
    val expiration: Instant,
    var deadline: Instant = Instant.now().plusMillis(100),
    val arrival: Instant = Instant.now()
) : CompletableFuture<Receipt>() {
    init {
        require(byteSize <= SIZE_THRESHOLD) { "record size cannot be larger than 1MiB" }
    }

    // Kinesis considers the partition/hash key part of the 1MB limit.
    val byteSize: Int
        get() = raw.size + 39

    val isExpired: Boolean
        get() = expiration.isBefore(Instant.now())

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Record) return false

        if (!raw.contentEquals(other.raw)) return false
        if (expiration != other.expiration) return false
        if (deadline != other.deadline) return false
        if (arrival != other.arrival) return false

        return true
    }

    override fun hashCode(): Int {
        var result = raw.contentHashCode()
        result = 31 * result + expiration.hashCode()
        result = 31 * result + deadline.hashCode()
        result = 31 * result + arrival.hashCode()
        return result
    }
}

internal class DeadlineComparator : Comparator<Record> {
    override fun compare(o1: Record, o2: Record): Int = o1.deadline.compareTo(o2.deadline)
}

internal class ArrivalComparator : Comparator<Record> {
    override fun compare(o1: Record, o2: Record): Int = o1.arrival.compareTo(o2.arrival)
}
