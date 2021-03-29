package com.nickdala

import java.nio.file.Paths

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode.Append

object Ingest extends App {

  if (args.length != 2) {
    println("Usage: Ingest <base path> <data file>")
    System.exit(1)
  }

  val basePath = Paths.get(args(0))
  val tableName = "scores_cow"
  val tablePath = "file://" + basePath + "/" + tableName

  val scoreData = args(1)

  println("tablePath: " + tablePath)
  println("scoreData: " + scoreData)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("HudiIngest")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate();

  val df = spark.read.format("json")
    .option("inferSchema", "true")
    .load(scoreData)

  //Specify common DataSourceWriteOptions in the single hudiOptions variable
  val hudiOptions = Map[String,String](
    HoodieWriteConfig.TABLE_NAME -> tableName,
    DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "name",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "update_time"
  )

  df.write.format("org.apache.hudi")
    .options(hudiOptions)
    .mode(Append)
    .save(tablePath)
}
