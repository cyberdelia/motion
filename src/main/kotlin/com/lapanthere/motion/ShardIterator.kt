package com.lapanthere.motion

import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.ShardFilterType
import java.util.concurrent.CompletableFuture
import java.util.concurrent.locks.ReentrantLock

internal class ShardIterator(
    private val name: String,
    private val kinesis: KinesisAsyncClient = KinesisAsyncClient.create(),
) : Iterator<Shard> {
    private companion object {
        private var logger = LoggerFactory.getLogger(ShardIterator::class.java)
    }

    private val shards = mutableSetOf<Shard>()
    private val lock = ReentrantLock()

    init {
        refresh().get()
    }

    override fun hasNext(): Boolean = shards.isNotEmpty()

    override fun next(): Shard =
        iterator {
            while (true) {
                shards.find { it.hasCapacity }?.let {
                    yield(it)
                }
            }
        }.next()

    fun invalidate() {
        logger.debug("received invalidation request")
        if (lock.tryLock()) {
            logger.debug("updating shards list")
            refresh().whenComplete { _, _ -> lock.unlock() }
        } else {
            logger.debug("shards list update already in-flight, skipping")
        }
    }

    internal fun refresh(): CompletableFuture<Unit> =
        kinesis.listShards {
            it.streamName(name).shardFilter { filter -> filter.type(ShardFilterType.AT_LATEST) }
        }.handle { response, e ->
            if (e == null) {
                val openShards = response.shards().map { Shard(it.shardId(), it.hashKeyRange().endingHashKey()) }.toSet()
                // This retains existing shards' representation (to avoid loosing track of quotas).
                shards.addAll(openShards)
                shards.retainAll(openShards)
            } else {
                logger.error("failed to update the shard list, retrying", e)
            }
        }
}
