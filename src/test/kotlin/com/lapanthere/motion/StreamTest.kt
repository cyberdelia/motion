package com.lapanthere.motion

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry
import software.amazon.awssdk.services.kinesis.model.Shard
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException
import java.util.function.Consumer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JacksonSerializer<V> : Serializer<V> {
    override fun serialize(value: V): ByteArray = jacksonObjectMapper().writeValueAsBytes(value)
}

internal class StreamTest {
    private val listResponse = ListShardsResponse.builder()
        .shards(
            Shard.builder()
                .shardId("shardId-000000000001")
                .hashKeyRange {
                    it.startingHashKey("34028236692093846346337460743176821145")
                        .endingHashKey("68056473384187692692674921486353642280")
                }
                .sequenceNumberRange {
                    it.startingSequenceNumber("49579844037727333356165064238440708846556371693205002258")
                }.build(),
            Shard.builder()
                .shardId("shardId-000000000002")
                .hashKeyRange {
                    it.startingHashKey("68056473384187692692674921486353642281")
                        .endingHashKey("102084710076281539039012382229530463436")
                }
                .sequenceNumberRange {
                    it.startingSequenceNumber("49579844037749634101363594861582244564829020124710982690")
                }.build()
        ).build()
    private val failureResponse = PutRecordsResponse.builder()
        .failedRecordCount(1)
        .records(
            PutRecordsResultEntry.builder()
                .shardId("shardId-000000000001")
                .errorCode("InternalFailure")
                .errorMessage("Internal service failure.")
                .build()
        )
        .build()
    private val partialResponse = PutRecordsResponse.builder()
        .failedRecordCount(1)
        .records(
            PutRecordsResultEntry.builder()
                .shardId("shardId-000000000001")
                .sequenceNumber("49543463076548007577105092703039560359975228518395019266")
                .build(),
            PutRecordsResultEntry.builder()
                .shardId("shardId-000000000001")
                .errorCode("InternalFailure")
                .errorMessage("Internal service failure.")
                .build(),
            PutRecordsResultEntry.builder()
                .shardId("shardId-000000000001")
                .errorCode("ProvisionedThroughputExceededException")
                .errorMessage("Rate exceeded for shard shardId-000000000001 in stream exampleStreamName under account 111111111111.")
                .build()
        )
        .build()

    private val kinesis: KinesisAsyncClient = mockk {
        every { listShards(any<Consumer<ListShardsRequest.Builder>>()) } returns
            CompletableFuture.completedFuture(listResponse)
        every { putRecords(any<Consumer<PutRecordsRequest.Builder>>()) } answers { call ->
            val builder = PutRecordsRequest.builder()
            (call.invocation.args.first() as Consumer<PutRecordsRequest.Builder>).accept(builder)
            val request = builder.build()
            val records = request.records().size
            val shard = when (request.records().first().explicitHashKey()) {
                "34028236692093846346337460743176821145" -> "shardId-000000000001"
                "68056473384187692692674921486353642281" -> "shardId-000000000002"
                else -> "shardId-000000000001"
            }
            CompletableFuture.completedFuture(
                PutRecordsResponse.builder()
                    .failedRecordCount(0)
                    .records(
                        (1..records).map {
                            PutRecordsResultEntry.builder()
                                .shardId(shard)
                                .sequenceNumber("49543463076548007577105092703039560359975228518395019266")
                                .build()
                        }
                    )
                    .build()
            )
        }
    }

    @Test
    fun `complete exceptionally after timeout`() {
        every { kinesis.putRecords(any<Consumer<PutRecordsRequest.Builder>>()) } returns
            CompletableFuture.completedFuture(failureResponse)
        Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis).use {
            val receipt = it.publish(emptyMap(), expires = Duration.ofMillis(125))
            assertThrows<ExecutionException> { receipt.get() }
        }
    }

    @Test
    fun `return receipt on success`() {
        Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis).use {
            val receipt = it.publish(emptyMap(), expires = Duration.ofMillis(125)).get()
            assertTrue(receipt.shardID in arrayOf("shardId-000000000001", "shardId-000000000002"))
            assertTrue(
                receipt.sequenceNumber in arrayOf(
                    "49579844037749634101363594861582244564829020124710982690",
                    "49543463076548007577105092703039560359975228518395019266"
                )
            )
        }
    }

    @Test
    fun `return receipt per record`() {
        every { kinesis.putRecords(any<Consumer<PutRecordsRequest.Builder>>()) } returns
            CompletableFuture.completedFuture(partialResponse)
        Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis).use {
            val successReceipt = it.publish(mapOf<String, Any?>("value" to 1), expires = Duration.ofMillis(101))
            val internalReceipt = it.publish(mapOf<String, Any?>("value" to 2), expires = Duration.ofMillis(101))
            val throughputReceipt = it.publish(mapOf<String, Any?>("value" to 2), expires = Duration.ofMillis(101))
            successReceipt.get()
            assertThrows<ExecutionException> { internalReceipt.get() }
            assertThrows<ExecutionException> { throughputReceipt.get() }
        }
    }

    @Test
    fun `flush when reaching batch count threshold`() {
        Stream<Map<String, Any?>>(
            "test",
            deadline = Duration.ofSeconds(1),
            serializer = JacksonSerializer(),
            kinesis = kinesis
        ).use { stream ->
            repeat(COUNT_THRESHOLD + 1) { stream.publish(ByteArray(0)) }
        }
        verify(exactly = 2) { kinesis.putRecords(any<Consumer<PutRecordsRequest.Builder>>()) }
    }

    @Test
    fun `expose the number of records in the queue`() {
        Stream<Map<String, Any?>>(
            "test",
            deadline = Duration.ofSeconds(1),
            serializer = JacksonSerializer(),
            kinesis = kinesis
        ).use { stream ->
            assertEquals(0, stream.pending)
            stream.publish(emptyMap())
            assertEquals(1, stream.pending)
        }
    }

    @Test
    fun `expose the age of the oldest records in the queue`() {
        Stream<Map<String, Any?>>(
            "test",
            deadline = Duration.ofSeconds(1),
            serializer = JacksonSerializer(),
            kinesis = kinesis
        ).use { stream ->
            assertEquals(Duration.ZERO, stream.age)
            stream.publish(emptyMap())
            assertTrue(stream.age > Duration.ZERO)
        }
    }

    @Test
    fun `flush when reaching batch size threshold`() {
        Stream<Map<String, Any?>>(
            "test",
            deadline = Duration.ofSeconds(1),
            serializer = JacksonSerializer(),
            kinesis = kinesis
        ).use { stream ->
            repeat(10) { stream.publish(ByteArray(SIZE_THRESHOLD / 8)) }
        }
        verify(exactly = 2) { kinesis.putRecords(any<Consumer<PutRecordsRequest.Builder>>()) }
    }

    @Test
    fun `ensures queue is emptied on close`() {
        val receipt = Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis).use {
            it.publish(emptyMap(), expires = Duration.ofMillis(250))
        }
        assertDoesNotThrow { receipt.get() }
    }

    @Test
    fun `throw an error if we're trying to publish after closure`() {
        val stream = Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis)
        stream.close()
        assertThrows<IllegalStateException> { stream.publish(emptyMap()) }
    }

    @Test
    fun `provides rate limit per shard`() {
        Stream<Map<String, Any?>>("test", serializer = JacksonSerializer(), kinesis = kinesis).use { stream ->
            val futures = (0..5000).map { stream.publish(byteArrayOf(), expires = Duration.ofSeconds(1)) }
            assertThrows<CompletionException> { CompletableFuture.allOf(*futures.toTypedArray()).join() }
        }
    }
}
