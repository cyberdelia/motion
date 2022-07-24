package com.lapanthere.motion

import java.time.Duration
import java.time.Instant
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit

internal class DeadlineQueue(
    private val deadline: Duration
) {
    private val queue = PriorityBlockingQueue(10, DeadlineComparator())

    fun add(record: Record): Boolean = queue.add(record)

    fun isEmpty(): Boolean = queue.isEmpty()

    fun requeue(record: Record): Boolean {
        record.deadline = minOf(Instant.now().plus(deadline.dividedBy(2)), record.expiration)
        return queue.offer(record)
    }

    fun requeue(records: List<Record>): Boolean = records.map {
        requeue(it)
    }.any()

    fun drain(max: Int, timeout: Duration): Collection<Record> = sequence {
        for (i in 0 until queue.size.coerceAtMost(max).coerceAtLeast(1)) {
            val record = queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS)
            if (record != null) {
                yield(record)
            }
        }
    }.toList()
}
