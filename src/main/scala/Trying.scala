import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder
import org.apache.flink.streaming.connectors.cassandra.CassandraSinkBase
// import org.apache.flink.streaming.connectors.cassandra.CassandraSource
import com.datastax.driver.core.Cluster


object Trying extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val kafkaSource = KafkaSource.builder()
  .setBootstrapServers("localhost:9092")
  .setTopics("flinkex")
  .setGroupId("flink-consumer-group")
  .setStartingOffsets(OffsetsInitializer.latest())
  .setValueOnlyDeserializer(new SimpleStringSchema())
  .build()
  println("Checkpoint 1")

  val serializer = KafkaRecordSerializationSchema.builder()
  .setValueSerializationSchema(new SimpleStringSchema())
  .setTopic("flinkout")
  .build()

  val cassandraHost = "localhost"
  val cassandraPort = 9042
  val cassandraKeyspace = "flink_db"
  val cassandraTable = "teachers"

  println("Checkpoint 2")
  val stream = env.addSource(new CassandraSourceFunction)
  println("Checkpoint 3")
  stream.print()
  println("Checkpoint 4")

  // val clusterBuilder = Cluster.builder().addContactPoint(cassandraHost).withPort(cassandraPort)
  // val cassandraInputFormat = new CassandraSource(cassandraHost, cassandraPort, cassandraKeyspace, cassandraTable, clusterBuilder)

  val kafkaSink = KafkaSink.builder()
  .setBootstrapServers("localhost:9092")
  .setRecordSerializer(serializer)
  .build()
  println("Checkpoint 5")

  val stream2 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
  // stream.print()
  // stream.sinkTo(kafkaSink)

  println("Checkpoint 6")
  env.execute("Trying")

}
