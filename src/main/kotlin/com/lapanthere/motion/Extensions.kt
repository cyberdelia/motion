package com.lapanthere.motion

import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse

/**
 * Returns true if all records have been published to the shard we picked.
 */
internal fun PutRecordsResponse.hasExpectedShard(shard: Shard): Boolean =
    records().filter { it.errorCode() == null }.all { it.shardId() == shard.name }
