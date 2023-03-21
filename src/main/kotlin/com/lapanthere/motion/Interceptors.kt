package com.lapanthere.motion

import org.slf4j.LoggerFactory

internal class Interceptors<V>(private val interceptors: List<Interceptor<V>>) {
    private companion object {
        private var logger = LoggerFactory.getLogger(Interceptor::class.java)
    }

    fun beforePublication(
        record: V,
        context: RecordContext = RecordContext(),
    ): Pair<V, RecordContext> =
        Pair(
            interceptors.fold(record) { acc, next ->
                try {
                    next.beforePublication(record, context)
                } catch (e: Exception) {
                    logger.warn("interceptor execution failed", e)
                    acc
                }
            },
            context,
        )

    internal fun afterPublication(receipt: Receipt?, throwable: Throwable?, context: Context) =
        interceptors.forEach {
            try {
                it.afterPublication(receipt, throwable, context)
            } catch (e: Exception) {
                logger.warn("interceptor execution failed", e)
            }
        }
}
