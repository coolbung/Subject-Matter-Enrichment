import org.apache.log4j.Logger
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
//import org.apache.flink.streaming.connectors.cassandra.CassandraSink
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
// import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder
// import org.apache.flink.streaming.connectors.cassandra.CassandraSinkBase
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
// import org.apache.flink.table.api._
// import org.apache.flink.table.api.bridge.scala._
// import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row
//import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import java.util
import java.util.Properties

import org.apache.spark._
import org.apache.spark.sql._
//import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
// import org.apache.flink.streaming.connectors.cassandra.CassandraSource
//import com.datastax.driver.core.Cluster


object Trying extends App {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // val invoiceSchema: TableSchema = TableSchema.builder()
  // .field("Student_Id", DataTypes.STRING())
  // .field("Student_Name", DataTypes.STRING())
  // .field("Doubt", DataTypes.STRING())
  // .field("Domain", DataTypes.STRING())
  // .build()

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

  // teachersDb.printSchema()
  // teachersDb.show()

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

  // println("Checkpoint 2")
  // val stream = env.addSource(new CassandraSourceFunction)
  // println("Checkpoint 3")
  // stream.print()
  // println("Checkpoint 4")

  // val clusterBuilder = Cluster.builder().addContactPoint(cassandraHost).withPort(cassandraPort)
  // val cassandraInputFormat = new CassandraSource(cassandraHost, cassandraPort, cassandraKeyspace, cassandraTable, clusterBuilder)

  val kafkaSink = KafkaSink.builder()
  .setBootstrapServers("localhost:9092")
  .setRecordSerializer(serializer)
  .build()
  println("Checkpoint 5")

  val stream2 = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

  // val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

// Convert the DataStream[String] to a Table
  // val table: Table = stream2.toTable(tEnv, 'jsonString)
  // table.printSchema()
  // table.print()
  // val resultTable: Table = table
  // .select('jsonString,
  //   from_json('jsonString, invoiceSchema) as 'invoice
  // )
  // .select('invoice("Student_Id"), 'invoice("Doubt"), 'invoice("Domain"))

  // val resultStream: DataStream[(String, String, String)] = resultTable
  // .toAppendStream[(String, String, String)]

// Print the result (for testing)
// resultStream.print()

  stream2.print()
  //stream2.sinkTo(kafkaSink)

  println("Checkpoint 6")
  env.execute("Trying")

}
