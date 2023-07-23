import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object Trying extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // val properties = new Properties()
  // properties.setProperty("bootstrap.servers", "localhost:9092")
  // properties.setProperty("group.id", "test")
  val kafkaSource = KafkaSource.builder()
  .setBootstrapServers("localhost:9092")
  .setTopics("flink-example")
  .setGroupId("flink-consumer-group")
  .setStartingOffsets(OffsetsInitializer.latest())
  .setValueOnlyDeserializer(new SimpleStringSchema())
  .build()

  val serializer = KafkaRecordSerializationSchema.builder()
  .setValueSerializationSchema(new SimpleStringSchema())
  .setTopic("flink-example-out")
  .build()

  val kafkaSink = KafkaSink.builder()
  .setBootstrapServers("localhost:9092")
  .setRecordSerializer(serializer)
  .build()

  val stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
  stream.print()
  env.execute("Trying")

  println("Hello, world!")
}
