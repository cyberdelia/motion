package com.lapanthere.motion

import java.io.Closeable

/**
 * An interface for converting objects to bytes.
 *
 * @param <V> Type to be serialized from.
 */
public fun interface Serializer<V> : Closeable {
    public fun serialize(value: V): ByteArray
    override fun close() {}
}
