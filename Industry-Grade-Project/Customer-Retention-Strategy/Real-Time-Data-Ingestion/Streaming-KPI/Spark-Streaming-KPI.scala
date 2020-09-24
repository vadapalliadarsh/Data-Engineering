package streamkpi

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SparkSession

object RealTimeStreamKPI {

def streamSurvey( caseDF : DataFrame) : Unit = {

// ----------------------- generating kpis on real time data -----------------------
import spark.implicits._
println(" ---------------------------------------------------------------- ")
println(" ")
println("• Total numbers of cases that are open and closed out of the number of cases received")
println(" ")
caseDF.groupBy(col("status")).count.show()
println(" ")
println(" ")

// ----------------------- get spark session -----------------------
val spark = Utils.get_spark_session("Read Hive Dimension Tables")

// ----------------------- fetching case category dimension data from -----------------------
caseCategoryDF = spark.sql("select distinct category_key, sub_category_key, priority from edureka_653389.futurecart_case_category_details")

// ----------------------- fetching case priority dimension data from -----------------------
casePriorityDF = spark.sql("select distinct priority, severity from edureka_653389.futurecart_case_priority_details")

// ----------------------- case category && case priority dataframe -----------------------
joinDF = caseCategoryDF.join(casePriorityDF, caseCategoryDF("priority") === casePriorityDF("priority"), "inner")

// ----------------------- join caseDF with joinDF-----------------------
resultDF = caseDF.join(joinDF, caseDF("category_key") === caseDF("category_key") && caseDF("sub_category_key") === caseDF("sub_category_key"), "inner")

println("• Total number of cases received based on priority and severity")
println(" ")
resultDF.groupBy(col("priority"), col("severity")).count.show()
println(" ")
println(" ---------------------------------------------------------------- ")
}
}




