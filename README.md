# Hudi Incremental Data Processing

Apache Hudi example demonstrating incremental data processing.

## Prerequisites

This example works with Spark 3.1.1.  Download Spark 3.1.1 from [here](https://spark.apache.org/downloads.html). 

1. Unpack the Spark archive

    ```tar -xzvf spark-3.1.1-bin-hadoop3.2.tgz```
    
2. Set SPARK_HOME variable.  Below is an example where Spark 3.1.1 was unpacked in `/Users/nickdala/stuff`
 
    ```export SPARK_HOME="/Users/nickdala/stuff/spark-3.1.1-bin-hadoop3.2"```

3. Add Spark bin directory to PATH variables    

    ```export PATH=$SPARK_HOME/bin:$PATH```
    
4. Verify that the prerequisite steps have been executed correctly 

    ```which spark-submit``` 
    
    You should see spark-submit located in your SPARK_HOME directory
    
    ```/Users/nickdala/stuff/spark-3.1.1-bin-hadoop3.2/bin/spark-submit```
 
## Build

```bash
sbt assembly
```

We can now issue `spak-submit` commands using `hudi-incremental-data-processing-assembly-0.1.jar` under `target/scala-2.12`

## Run Spark Jobs

Let's first run the ingest job twice.  

1. Execute `spark-submit` ingesting `data/1.json`.

    ```bash
    spark-submit \
      --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 \
      --class com.nickdala.Ingest \
      local:///Users/nickdala/git/hudi-incremental-data-processing/target/scala-2.12/hudi-incremental-data-processing-assembly-0.1.jar /Users/nickdala/git/hudi-incremental-data-processing/hudi /Users/nickdala/git/hudi-incremental-data-processing/data/1.json
    ```

1. Verify we ingested `data/1.json` using the interactive Spark shell.
    
   ```bash
    spark-shell \
      --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 \
      --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
    ```

    You should see the following.
    
    ```bash
    21/03/28 20:33:14 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    Spark context Web UI available at http://nikolaoss-mbp.fios-router.home:4040
    Spark context available as 'sc' (master = local[*], app id = local-1616978000414).
    Spark session available as 'spark'.
    Welcome to
          ____              __
         / __/__  ___ _____/ /__
        _\ \/ _ \/ _ `/ __/  '_/
       /___/ .__/\_,_/_/ /_/\_\   version 3.1.1
          /_/
             
    Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 11.0.7)
    Type in expressions to have them evaluated.
    Type :help for more information.
    
    scala> 
    ```
   
 1. Query the `scores_cow` table

    ```bash
    scala> val basePath = "/Users/nickdala/git/hudi-incremental-data-processing/hudi/scores_cow"
    scala> val scoresDF = spark.read.format("hudi").load(basePath + "/*")
    scala> scoresDF.createOrReplaceTempView("scores_snapshot")
    ```
    
    ```bash
    scala> spark.sql("select * from scores_snapshot").show()
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|  name|score|        update_time|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |     20210328151752|  20210328151752_0_1|              Nick|               default|c5d83e23-6591-4d9...|  Nick|   25|2021-01-08 09:00:00|
    |     20210328151752|  20210328151752_0_2|            Steven|               default|c5d83e23-6591-4d9...|Steven|   12|2021-01-08 09:00:00|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    ```

1. Ingest `data/2.json`

    ```bash
    spark-submit \
      --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 \
      --class com.nickdala.Ingest \
      local:///Users/nickdala/git/hudi-incremental-data-processing/target/scala-2.12/hudi-incremental-data-processing-assembly-0.1.jar /Users/nickdala/git/hudi-incremental-data-processing/hudi /Users/nickdala/git/hudi-incremental-data-processing/data/2.json
    ```
1. Verify we ingested `data/2.json` using the interactive Spark shell we previously launched.

    ```bash
    scala> val basePath = "/Users/nickdala/git/hudi-incremental-data-processing/hudi/scores_cow"
    scala> val scoresDF = spark.read.format("hudi").load(basePath + "/*")
    scala> scoresDF.createOrReplaceTempView("scores_snapshot")
    ```

    ```bash
    scala> spark.sql("select * from scores_snapshot").show()
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|  name|score|        update_time|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |     20210328151752|  20210328151752_0_1|              Nick|               default|c5d83e23-6591-4d9...|  Nick|   25|2021-01-08 09:00:00|
    |     20210328151752|  20210328151752_0_2|            Steven|               default|c5d83e23-6591-4d9...|Steven|   12|2021-01-08 09:00:00|
    |     20210328204501|  20210328204501_0_1|            Angela|               default|c5d83e23-6591-4d9...|Angela|   11|2021-01-08 10:00:00|
    |     20210328204501|  20210328204501_0_2|              Lexi|               default|c5d83e23-6591-4d9...|  Lexi|   18|2021-01-08 10:00:00|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    ```

1. Run the Incremental spark job

    ```bash
    spark-submit \
      --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 \
      --class com.nickdala.Incremental \
      local:///Users/nickdala/git/hudi-incremental-data-processing/target/scala-2.12/hudi-incremental-data-processing-assembly-0.1.jar /Users/nickdala/git/hudi-incremental-data-processing/hudi
    ```

1. Query `scores_incremental_cow` Hudu table using the interactive Spark shell we previously launched.
 
    ```bash
    scala> val incrementalPath = "/Users/nickdala/git/hudi-incremental-data-processing/hudi/scores_incremental_cow"
    scala> val scoresIncrementalDF = spark.read.format("hudi").load(incrementalPath + "/*")
    scala> scoresIncrementalDF.createOrReplaceTempView("scores_incremental")
    scala> spark.sql("select * from scores_incremental").show()
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|  name|score|        update_time|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    |     20210328210107|  20210328210107_0_1|              Nick|               default|030df7fc-060a-47f...|  Nick|   25|2021-01-08 09:00:00|
    |     20210328210107|  20210328210107_0_2|            Angela|               default|030df7fc-060a-47f...|Angela|   11|2021-01-08 10:00:00|
    |     20210328210107|  20210328210107_0_3|            Steven|               default|030df7fc-060a-47f...|Steven|   12|2021-01-08 09:00:00|
    |     20210328210107|  20210328210107_0_4|              Lexi|               default|030df7fc-060a-47f...|  Lexi|   18|2021-01-08 10:00:00|
    +-------------------+--------------------+------------------+----------------------+--------------------+------+-----+-------------------+
    ```

## Incremental Processing 

Ingest 3.json

```bash
spark-submit \
  --packages org.apache.hudi:hudi-spark-bundle_2.12:0.7.0,org.apache.spark:spark-avro_2.12:3.0.1 \
  --class com.nickdala.Ingest \
  local:///Users/nickdala/git/hudi-incremental-data-processing/target/scala-2.12/hudi-incremental-data-processing-assembly-0.1.jar /Users/nickdala/git/hudi-incremental-data-processing/hudi /Users/nickdala/git/hudi-incremental-data-processing/data/3.json
```

Verify new data was upserted in the scores_cow table

```bash
scala> val scoresDF = spark.read.format("hudi").load(basePath + "/*")
21/03/28 21:10:04 WARN DefaultSource: Loading Base File Only View.
scoresDF: org.apache.spark.sql.DataFrame = [_hoodie_commit_time: string, _hoodie_commit_seqno: string ... 6 more fields]

scala> scoresDF.createOrReplaceTempView("scores_snapshot")

scala> spark.sql("select * from scores_snapshot").show()
+-------------------+--------------------+------------------+----------------------+--------------------+-------+-----+-------------------+
|_hoodie_commit_time|_hoodie_commit_seqno|_hoodie_record_key|_hoodie_partition_path|   _hoodie_file_name|   name|score|        update_time|
+-------------------+--------------------+------------------+----------------------+--------------------+-------+-----+-------------------+
|     20210328151752|  20210328151752_0_1|              Nick|               default|c5d83e23-6591-4d9...|   Nick|   25|2021-01-08 09:00:00|
|     20210328151752|  20210328151752_0_2|            Steven|               default|c5d83e23-6591-4d9...| Steven|   12|2021-01-08 09:00:00|
|     20210328204501|  20210328204501_0_1|            Angela|               default|c5d83e23-6591-4d9...| Angela|   11|2021-01-08 10:00:00|
|     20210328204501|  20210328204501_0_2|              Lexi|               default|c5d83e23-6591-4d9...|   Lexi|   18|2021-01-08 10:00:00|
|     20210328210855|  20210328210855_0_1|           Chrissy|               default|c5d83e23-6591-4d9...|Chrissy|   22|2021-01-09 09:30:00|
+-------------------+--------------------+------------------+----------------------+--------------------+-------+-----+-------------------+
```

***TODO: How do we modify Incremental.scala to stream new changes***
