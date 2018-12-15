/*
Instructions
Get word count for the input data using space as delimiter (for each word, we need to get how many times it is repeated in the entire input data set)

Data Description
Data is available in HDFS /public/randomtextwriter

word count data information:

Number of executors should be 10
executor memory should be 3 GB
Executor cores should be 20 in total (2 per executor)
Number of output files should be 8
Avro dependency details: groupId -> com.databricks, artifactId -> spark-avro_2.10, version -> 2.0.1
Output Requirements
Output File format: Avro
Output fields: word, count
Compression: Uncompressed
Place the customer files in the HDFS directory
/user/`whoami`/problem5/solution/
Replace `whoami` with your OS user name
End of Problem
*/

spark-shell --master yarn --conf spark.ui.port=25586 --num-executors 10 --executor-cores 2 --executor-memory 3G --packages com.databricks:spark-avro_2.10:2.0.1

import com.databricks.spark.avro._

sc.setLogLevel("ERROR")
sqlContext.setConf("spark.sql.shuffle.partitions","8")

val randomtextwriter = sc.textFile("/public/randomtextwriter")

val randomtextwriterMap = randomtextwriter.
                                          flatMap(_.split(" ")).
                                          map(x => (x, 1))

val wordCount = randomtextwriterMap.reduceByKey(_+_).sortBy(x => -x._2)

val wordCountDF = wordCount.toDF("words", "wordCount")
wordCountDF.cache 



wordCountDF.repartition(8).write.mode("overwrite").avro("/user/vivekvarun20/problem5/solution/")
