import org.apache.log4j.Logger
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.types.Row
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.cassandra._
import org.apache.spark._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._


import java.util
import java.util.Properties


object Final extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  case class Invoice(
    student_id: String,
    doubt: String,
    domain: String
  )

  val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Trying")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()

  val teachersDb = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", "flink_db")
      .option("table", "teachers")
      .load()

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

  val kafkaSink = KafkaSink.builder()
  .setBootstrapServers("localhost:9092")
  .setRecordSerializer(serializer)
  .build()

  val stream2 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
  //println(teachersDb.select("domains").show())

  stream2.print()
  val stream1 = stream2.map(x => {
    val arr = x.split(",")
    val student_id = arr(0)
    val doubt = arr(1)
    val domain = arr(2)
    println(domain)
    // val foundTeachers = teachersDb.filter(array_contains(col("domains"), domain))
    val foundTeachers = teachersDb.filter(array_contains(col("domains"), domain))
    import spark.implicits._
    val teacherIdList: List[String] = foundTeachers.select("teacher_id").as[String].collect().toList

    println(teacherIdList)

    val combined = teacherIdList.map(x => {
      (x, student_id, doubt, domain)
    })
    combined
    

  })


  val formattedStream: DataStream[String] = stream1.flatMap(_.map {
    case (a, b, c, d) => s"$a,$b,$c,$d"
  }).map(_.mkString(""))

  formattedStream.print()
  formattedStream.sinkTo(kafkaSink)

  // stream1.sinkTo(kafkaSink)
  env.execute("Final")

}

