+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

mkdir streaming
mkdir streaming/src
mkdir streaming/src/main
mkdir streaming/src/main/scala

vi industry_grade_project/streaming/build.sbt

name := "stream"
description := ""
version := "0.1"
scalaVersion := "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % "provided"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.12"


+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

vi streaming/src/main/scala/stream.scala

package stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.SparkSession
import utilities.Utils
import realtimeload.LoadRealTimeStream
import streamkpi.RealTimeStreamKPI

object RealTimeStream {

def main (args : Array[String]) : Unit = {
// ----------------------- Spark session -----------------------
val spark = Utils.get_spark_session("Data streaming")
val ssc = new StreamingContext(spark.sparkContext, Seconds(30))

// ----------------------- stream processing -----------------------
streamSurvey(ssc)
streamCase(ssc)

// ----------------------- Begin stream  -----------------------
try{
  ssc.start()
  ssc.awaitTermination()
} finally {
  ssc.stop(true, true)
}
}


def streamSurvey( ssc : StreamingContext) : Unit = {

// ----------------------- configuring input stream -----------------------
val topics = "SurveyTopic"
val numThreads = 4
val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
val input_stream = KafkaUtils.createStream(ssc, "ip-20-0-31-221.ec2.internal:9092", "group_1", topicMap)
//val input_stream = KafkaUtils.createStream(ssc,"ip-20-0-31-221.ec2.internal:9092", "group_1", Map("SurveyTopic" -> 1))

// ----------------------- Schema definition -----------------------
val jsonSchemaSurvey =  Utils.getSchemaSurvey()

import spark.implicits._
val surveyStream = input_stream.map( x => x._2.toString()).foreachRDD( rdd => {
    if(!rdd.isEmpty()){
      val df = rdd.toDF("value")

      // ----------------------- validate json. Non adherent records are rejected  -----------------------
      //val streamDF = df.select(from_json(col("value"), jsonSchemaSurvey).as("value")).filter(col("value").isNotNull).select("value.*")

      // ----------------------- Load Data into Cassandra -----------------------
      LoadRealTimeStream.loadSurvey(spark, streamDF)}
	}
	)
}


def streamCase( ssc : StreamingContext) : Unit = {

// ----------------------- configuring input stream -----------------------
val topics = "CaseTopic"
val numThreads = 4
val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
val input_stream = KafkaUtils.createStream(ssc, "ip-20-0-31-221.ec2.internal:9092", "group_1", topicMap)
//val input_stream = KafkaUtils.createStream(ssc,"ip-20-0-31-221.ec2.internal:9092", "group_1", Map("CaseTopic" -> 1))

// ----------------------- Schema definition -----------------------
val jsonSchemaCase =  Utils.getSchemaCase()

import spark.implicits._
val caseStream = input_stream.map( x => x._2.toString()).foreachRDD( rdd => {
    if(!rdd.isEmpty()){
      val df = rdd.toDF("value")

      // ----------------------- validate json. Non adherent records are rejected  -----------------------
      //val caseDF = df.select(from_json(col("value"), jsonSchemaCase).as("value")).filter(col("value").isNotNull).select("value.*")

      // ----------------------- Load Data into Cassandra -----------------------
      LoadRealTimeStream.loadCase(spark, caseDF)}
	  
	  // ----------------------- generating kpis on real time data -----------------------
	  RealTimeStreamKPI.streamSurvey(caseDF)
	}
	)
}

}


sbt package
spark2-submit --class stream.RealTimeStream --deploy-mode client target/scala-2.11/stream_2.11-1.0.jar
