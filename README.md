# Motion

Motion is Kinesis producer optimized for random partition keys.

### Installation

Add the library to your gradle dependencies:

```kotlin
dependencies {
    implementation("com.lapanthere:motion:0.1")
}
```

### Usage

To create a stream:

```kotlin
val stream = Stream("kinesis-stream", serializer = { value -> objectMapper.writeValueAsBytes(value) })
```

To publish to the stream:

```kotlin
val future = stream.publish(event, expires = Duration.ofSeconds(1))
```

Ensure you close the stream before shutting down your application:

```kotlin
stream.close()
```

#### Interceptors

You can specify interceptors that are allowed to intercept records before and after publication.

Interceptors are also allowed to mutate the record before publication, each interceptor receives the record returned by
the previous interceptor in the chain.

```kotlin
class MetricsInterceptor : Interceptor<Record> {
    override fun beforePublication(record: Record, context: Context): Record {
        Metrics.count("kinesis.publication.count").increment()
        return record
    }

    override fun afterPublication(receipt: Receipt?, throwable: Throwable?, context: Context) {
        if (receipt != null) {
            Metrics.timer("kinesis.publication.latency", "shard_id", receipt.shardID)
                .record(receipt.duration)
        }
    }
}

val stream = Stream(
    "stream-name", serializer = { value -> objectMapper.writeValueAsBytes(value) },
    interceptors = listOf(MetricsInterceptor())
)
```

