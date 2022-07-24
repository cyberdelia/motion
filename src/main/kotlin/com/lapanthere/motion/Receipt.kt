package com.lapanthere.motion

import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry
import java.time.Duration
import java.time.Instant

/**
 * The result of an individual record published to the stream.
 *
 * A record that is successfully added to a stream includes a sequenceNumber and shardID in the result.
 *
 * @param sequenceNumber The sequence number for an individual record result.
 * @param shardID The shard ID for an individual record result.
 * @param duration Time it took to publish the record.
 *
 */
public data class Receipt(val sequenceNumber: String, val shardID: String, val duration: Duration) {
    internal constructor(entry: PutRecordsResultEntry, record: Record) : this(
        entry.sequenceNumber(),
        entry.shardId(),
        Duration.between(Instant.now(), record.arrival).abs()
    )
}
