import org.apache.flink.api.scala._

object Main extends App {
  val benv = ExecutionEnvironment.getExecutionEnvironment
  val dataSet = benv.readTextFile("C:/Projects/Flink course/OnlineRetail.csv")
  dataSet
    .filter(_.contains("United Kingdom")) // Correct the filtering keyword
    .map(line =>
      (
        line.split(",")(0).trim(), // Assuming the first column is the item identifier
        line.split(",")(2).trim()   // Assuming the third column is the item description
      ) 
    )
    .first(5).print()
}
