package com.lapanthere.motion

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.PriorityBlockingQueue

internal class RecordTest {
    private val now = Instant.now()
    private val older =
        Record(byteArrayOf(), deadline = now.minusMillis(1000), arrival = now.minusMillis(1000), expiration = now)
    private val current = Record(byteArrayOf(), deadline = now, arrival = now, expiration = now)
    private val newer =
        Record(byteArrayOf(), deadline = now.plusMillis(1000), arrival = now.plusMillis(1000), expiration = now)

    @Test
    fun `can compare by deadline`() {
        val comparator = DeadlineComparator()
        assertEquals(-1, comparator.compare(current, newer))
        assertEquals(0, comparator.compare(current, current))
        assertEquals(1, comparator.compare(current, older))
    }

    @Test
    fun `can compare by arrival`() {
        val comparator = ArrivalComparator()
        assertEquals(-1, comparator.compare(current, newer))
        assertEquals(0, comparator.compare(current, current))
        assertEquals(1, comparator.compare(current, older))
    }

    @Test
    fun `ensure buffer priority is based on the closest deadline`() {
        val buffer = PriorityBlockingQueue(10, DeadlineComparator())
        buffer.add(current)
        buffer.add(older)
        buffer.add(newer)
        assertEquals(3, buffer.size)
        assertEquals(older, buffer.poll())
        assertEquals(current, buffer.poll())
        assertEquals(newer, buffer.poll())
        assertTrue(buffer.isEmpty())
    }

    @Test
    fun `ensure queue priority is based on the closest deadline`() {
        val queue = PriorityBlockingQueue(10, ArrivalComparator())
        queue.add(newer)
        queue.add(current)
        queue.add(older)
        assertEquals(3, queue.size)
        assertEquals(older, queue.poll())
        assertEquals(current, queue.poll())
        assertEquals(newer, queue.poll())
        assertTrue(queue.isEmpty())
    }
}
