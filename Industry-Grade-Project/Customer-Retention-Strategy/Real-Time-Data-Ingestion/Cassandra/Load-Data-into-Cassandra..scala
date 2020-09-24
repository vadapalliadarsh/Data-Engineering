package realtimeload

import org.apache.spark.sql.cassandra._
//Spark connector
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

//CosmosDB library for multiple retry
import com.microsoft.azure.cosmosdb.cassandra

//Connection-related
spark.conf.set("spark.cassandra.connection.host","YOUR_ACCOUNT_NAME.cassandra.cosmosdb.azure.com")
spark.conf.set("spark.cassandra.connection.port","10350")
spark.conf.set("spark.cassandra.connection.ssl.enabled","true")
spark.conf.set("spark.cassandra.auth.username","YOUR_ACCOUNT_NAME")
spark.conf.set("spark.cassandra.auth.password","YOUR_ACCOUNT_KEY")
spark.conf.set("spark.cassandra.connection.factory", "com.microsoft.azure.cosmosdb.cassandra.CosmosDbConnectionFactory")
//Throughput-related...adjust as needed
spark.conf.set("spark.cassandra.output.batch.size.rows", "1")
spark.conf.set("spark.cassandra.connection.connections_per_executor_max", "10")
spark.conf.set("spark.cassandra.output.concurrent.writes", "1000")
spark.conf.set("spark.cassandra.concurrent.reads", "512")
spark.conf.set("spark.cassandra.output.batch.grouping.buffer.size", "1000")
spark.conf.set("spark.cassandra.connection.keep_alive_ms", "600000000")

object LoadRealTimeStream {

def loadSurvey( spark : SparkSession, surveyDF : DataFrame) : Unit = {
	surveyDF.write.mode("append").format("org.apache.spark.sql.cassandra").options(Map( "table" -> "survey", "keyspace" -> "edureka_653389", "output.consistency.level" -> "ALL", "ttl" -> "10000000")).save()
  }

def loadCase( spark : SparkSession, caseDF : DataFrame) : Unit = {
	caseDF.write.mode("append").format("org.apache.spark.sql.cassandra").options(Map( "table" -> "case", "keyspace" -> "edureka_653389", "output.consistency.level" -> "ALL", "ttl" -> "10000000")).save()
  }
}
