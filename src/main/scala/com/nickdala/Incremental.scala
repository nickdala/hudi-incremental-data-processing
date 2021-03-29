package com.nickdala

import org.apache.spark.sql.SparkSession
import java.nio.file.Paths

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_OPT_KEY}
import org.apache.spark.sql.SaveMode.Append

object Incremental extends App {

  if (args.length != 1) {
    println("Usage: Incremental <base path>")
    System.exit(1)
  }

  val basePath = Paths.get(args(0))
  val ingestTableName = "scores_cow"
  val ingestTablePath = "file://" + basePath + "/" + ingestTableName
  val incrementalTableName = "scores_incremental_cow"
  val incrementalTablePath = "file://" + basePath + "/" + incrementalTableName


  println("ingestTablePath: " + ingestTablePath)
  println("incrementalTablePath: " + incrementalTablePath)

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("HudiIncremental")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate();

  // TODO:  How do you incrementally pull from the source table?
  val beginTime = 0

  // incrementally query data
  val scoresIncrementalDF = spark.read.format("hudi")
    .option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL)
    .option(BEGIN_INSTANTTIME_OPT_KEY, beginTime)
    .load(ingestTablePath)

  //Specify common DataSourceWriteOptions in the single hudiOptions variable
  val hudiOptions = Map[String,String](
    HoodieWriteConfig.TABLE_NAME -> incrementalTableName,
    DataSourceWriteOptions.TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "name",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "update_time"
  )

  scoresIncrementalDF.write.format("org.apache.hudi")
    .options(hudiOptions)
    .mode(Append)
    .save(incrementalTablePath)

}
