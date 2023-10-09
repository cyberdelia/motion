package com.lapanthere.motion

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.LimitExceededException
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse
import software.amazon.awssdk.services.kinesis.model.Shard
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import kotlin.test.assertNotNull

internal class ShardIteratorTest {
    private val listResponse =
        ListShardsResponse.builder()
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
                    }.build(),
            ).build()
    private val record = Record(byteArrayOf(), expiration = Instant.now().plusSeconds(1))
    private val kinesis: KinesisAsyncClient =
        mockk {
            every { listShards(any<Consumer<ListShardsRequest.Builder>>()) } returns
                CompletableFuture.completedFuture(listResponse)
        }

    @Test
    fun `fetch shards when created`() {
        val iterator = ShardIterator(name = "stream", kinesis = kinesis)
        verify(exactly = 1) { kinesis.listShards(any<Consumer<ListShardsRequest.Builder>>()) }
        assertNotNull(iterator.next())
        assertNotNull(iterator.next())
    }

    @Test
    fun `does not empty shards list on failure`() {
        val iterator = ShardIterator(name = "stream", kinesis = kinesis)
        every { kinesis.listShards(any<Consumer<ListShardsRequest.Builder>>()) } returns
            CompletableFuture.failedFuture(LimitExceededException.create("Limit exceeded.", null))
        iterator.refresh().get()
        assertNotNull(iterator.next())
    }
}
