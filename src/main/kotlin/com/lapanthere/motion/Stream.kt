package com.lapanthere.motion

import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.InternalFailureException
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry
import java.io.Closeable
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.min

// Records threshold.
internal const val COUNT_THRESHOLD = 500

// Size threshold.
internal const val SIZE_THRESHOLD = 262_144

// Max size for a record.
internal const val MAX_RECORD_SIZE = 1_048_576

/**
 * Represents a kinesis stream to publish to.
 *
 * @param name name of the Kinesis stream.
 * @param deadline maximum time a record will be buffered in memory.
 * @param serializer a {@code Serializer} used to serialize a record.
 * @param interceptors a chain of `Interceptor`.
 * @param kinesis {@code KinesisAsyncClient} used to communicate with Kinesis.
 * @param executorService {@code ExecutorService} used to run the Producer.
 *
 * @constructor Creates a Stream.
 */
public class Stream<V>(
    private val name: String,
    private val deadline: Duration = Duration.ofMillis(100),
    private val serializer: Serializer<V>,
    interceptors: List<Interceptor<V>> = emptyList(),
    private val kinesis: KinesisAsyncClient = KinesisAsyncClient.create(),
    private val executorService: ExecutorService = Executors.newSingleThreadExecutor(),
) : Closeable {
    private val buffer = DeadlineQueue(deadline)
    private val queue = PriorityBlockingQueue(10, ArrivalComparator())
    private val interceptors = Interceptors(interceptors)
    private val producer = Producer()

    init {
        require((1..128).contains(name.length)) { "stream name must be between 1 and 128 characters maximum" }
        require(name.matches(Regex("[a-zA-Z0-9_.-]+"))) { "stream name must be be alphabetical" }
        require(!deadline.isZero) { "deadline cannot be set to zero" }
        require(!deadline.isNegative) { "deadline must be positive" }

        executorService.submit(producer)
    }

    private companion object {
        private var logger = LoggerFactory.getLogger(Stream::class.java)
    }

    /**
     * Returns a future to obtain the publishing receipt for the given record.
     *
     * @param record the object to be serialized as the payload.
     * @param expires record time-to-live.
     * @return a future for the publishing receipt
     *
     */
    public fun publish(
        record: V,
        expires: Duration = Duration.ofSeconds(30),
    ): Future<Receipt> {
        check(producer.isActive) { "publisher is already closed" }

        val (intercepted, context) = interceptors.beforePublication(record)
        return publish(serializer.serialize(intercepted), expires).whenComplete { receipt, exception ->
            interceptors.afterPublication(receipt, exception, RecordContext(context.properties))
        }
    }

    /**
     * Returns a future to obtain the publishing receipt for the given record.
     *
     * @param payload the payload to be published.
     * @param expires record time-to-live.
     * @return a future for the publishing receipt
     *
     */
    internal fun publish(
        payload: ByteArray,
        expires: Duration = Duration.ofSeconds(30),
    ): CompletableFuture<Receipt> {
        require(payload.size <= MAX_RECORD_SIZE) { "payload size must be less than or equal to 1MB" }
        require(expires > deadline) { "expiry must be greater than timeout" }
        return Record(payload, expiration = Instant.now() + expires).apply {
            buffer.add(this)
            queue.add(this)
            whenComplete { _, _ -> queue.remove(this) }
        }
    }

    /**
     * Returns the number of records waiting to be published.
     */
    public val pending: Int
        get() = queue.size

    /**
     * Returns the age of the oldest records waiting to be published.
     */
    public val age: Duration
        get() {
            val head = queue.peek()
            return if (head != null) {
                Duration.between(head.arrival, Instant.now()).abs()
            } else {
                Duration.ZERO
            }
        }

    /**
     * Shut down processing after making sure all records have been published if any.
     */
    override fun close() {
        producer.stop()
        serializer.close()
        executorService.shutdown()
        executorService.awaitTermination(deadline.toMillis(), TimeUnit.MILLISECONDS)
    }

    private inner class Producer : Runnable {
        private val iterator = ShardIterator(name, kinesis)

        var isActive: Boolean = true
            private set

        override fun run() {
            batches.forEach { (batch, shard) ->
                if (batch.isNotEmpty()) {
                    flush(batch, shard)
                }
            }
        }

        fun stop() {
            isActive = false
        }

        private val batches =
            iterator {
                var batch = emptyBatch()
                var shard = iterator.next()
                while (isActive || !buffer.isEmpty()) {
                    val records = buffer.drain(min(COUNT_THRESHOLD, shard.availableRecords), deadline.dividedBy(4))
                    for (record in records) {
                        if (record.isExpired) {
                            record.completeExceptionally(TimeoutException("couldn't publish record in time"))
                            continue
                        }

                        if (batch.byteSize + record.byteSize > SIZE_THRESHOLD ||
                            batch.size > COUNT_THRESHOLD - 1 ||
                            !shard.mayConsume(record.byteSize)
                        ) {
                            yield(Pair(batch, shard))
                            shard = iterator.next()
                            batch = emptyBatch()
                        }

                        if (shard.consume(record.byteSize)) {
                            batch.add(record)
                        } else {
                            buffer.requeue(record)
                        }
                    }

                    if (batch.isExpired) {
                        yield(Pair(batch, shard))
                        shard = iterator.next()
                        batch = emptyBatch()
                    }
                }

                // Ensure we flush all records left.
                yield(Pair(batch, shard))
            }

        private fun flush(
            batch: Batch,
            shard: Shard,
        ) {
            logger.debug("publishing ${batch.size} records to ${shard.name}")

            kinesis.putRecords {
                it.streamName(name).records(
                    batch.map { record ->
                        PutRecordsRequestEntry.builder()
                            .data(SdkBytes.fromByteArray(record.raw))
                            .partitionKey(UUID.randomUUID().toString())
                            .explicitHashKey(shard.explicitHashKey)
                            .build()
                    },
                )
            }.handle { response, e ->
                if (e == null) {
                    if (!response.hasExpectedShard(shard)) {
                        logger.debug("unexpected shard invalidating our cache")
                        iterator.invalidate()
                    }

                    if (response.records().size != batch.size) {
                        batch.completeExceptionally(
                            IllegalStateException("response contains ${response.records().size} but ${batch.size} were sent"),
                        )
                    } else {
                        response.records().zip(batch) { entry, record ->
                            if (entry.errorCode() == null) {
                                record.complete(Receipt(entry, record))
                            } else {
                                if (record.isExpired) {
                                    val exception =
                                        when (entry.errorCode()) {
                                            "InternalFailure" -> InternalFailureException.create(entry.errorMessage(), null)
                                            "ProvisionedThroughputExceededException" ->
                                                ProvisionedThroughputExceededException.create(entry.errorMessage(), null)
                                            else -> TimeoutException("couldn't publish record in time")
                                        }
                                    record.completeExceptionally(exception)
                                } else {
                                    buffer.requeue(record)
                                }
                            }
                        }
                    }
                } else {
                    logger.error("failed to publish, retrying", e)

                    buffer.requeue(batch.completeExceptionally(e))
                }
            }
        }
    }
}
