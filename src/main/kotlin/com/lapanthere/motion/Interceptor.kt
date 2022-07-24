package com.lapanthere.motion

/**
 * An interface that allows to intercept (and mutate) the records before
 * and after they are published to Kinesis.
 */
public fun interface Interceptor<V> {
    /**
     * This is called before a record is serialized. This method is allowed to modify the record,
     * and to store metadata in the given context.
     *
     * Any exception thrown by this method will be caught by the caller and logged, but not propagated further.
     *
     * @param record the record from client or the record returned by the previous interceptor in the chain of interceptors.
     * @param context A record context to store metadata.
     * @return the record to be published to Kinesis.
     */
    public fun beforePublication(record: V, context: Context): V = record

    /**
     * This is called when the record has been published, or when sending the record fails.
     *
     * Any exception thrown by this method will be ignored by the caller.
     *
     * @param receipt The publication receipt for the record that was published. `null` if an error occurred.
     * @param throwable The exception thrown during publication of this record. `null` if no error occurred.
     * @param context The same record context passed to the interceptor `beforcePublication` call.
     */
    public fun afterPublication(receipt: Receipt?, throwable: Throwable?, context: Context)

    /**
     * Close the interceptor if needed.
     */
    public fun close() {}
}
