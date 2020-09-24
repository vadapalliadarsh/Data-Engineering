package utilities

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Utils {

def getSchemaSurvey() : StructType = {
  StructType(
    Array(StructField("survey_id",StringType,true),
        StructField("Case_no",StringType,true),
        StructField("survey_timestamp",StringType,true),
        StructField("q1",StringType,true),
        StructField("q2",StringType,true),
        StructField("q3",StringType,true),
        StructField("q4",StringType,true),
        StructField("q5",StringType,true)
    ))
}


def getSchemaCase() : StructType = {
  StructType(
    Array(StructField("Case_no",StringType,true),
        StructField("create_timestamp",StringType,true),
        StructField("last_modified_timestamp",StringType,true),
		StructField("created_employee_key",StringType,true),
		StructField("call_center_id",StringType,true),
		StructField("status",StringType,true),
        StructField("category",StringType,true),
        StructField("sub_category",StringType,true),
        StructField("communication_mode",StringType,true),
        StructField("country_cd",StringType,true),
        StructField("product_code",StringType,true)
    ))
}


def get_spark_session(app_name : String) : SparkSession ={
  val spark =
  SparkSession.builder().
    config("spark.cassandra.connection.host","cassandradb.edu.cloudlab.com").
    config("spark.cassandra.connection.port",9042).
    appName(app_name).
    enableHiveSupport().
    getOrCreate()

  spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
  spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  return spark
}
}
