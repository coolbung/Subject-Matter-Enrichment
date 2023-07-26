import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.configuration.Configuration
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row

class CassandraSourceFunction extends RichSourceFunction[String] {
    println("Inside CassandraSourceFunction")
  private var isRunning: Boolean = true
  private var session: CqlSession = _

  override def open(parameters: Configuration): Unit = {
    println("Inside open")
    val sessionBuilder = CqlSession.builder()
      .withKeyspace("flink_db")
      .addContactPoint(new java.net.InetSocketAddress("localhost", 9042)) 
      .build()

    session = sessionBuilder
  }


  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val resultSet = session.execute("SELECT * FROM teachers") 
    val iterator = resultSet.iterator()
    while (isRunning && iterator.hasNext) {
      println("HIIIIIII")
      val row: Row = iterator.next()
      val data: String = row.getString("teacher_id") 
      println(data)
      ctx.collect(data)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def close(): Unit = {
    if (session != null) {
      session.close()
    }
  }
}
