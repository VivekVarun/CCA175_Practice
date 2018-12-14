/*
Instructions
Get top 3 crime types based on number of incidents in RESIDENCE area using "Location Description"

Data Description
Data is available in HDFS under /public/crime/csv

crime data information:

Structure of data: (ID, Case Number, Date, Block, IUCR, Primary Type, Description, Location Description, Arrst, Domestic, Beat, District, Ward, Community Area, FBI Code, X Coordinate, Y Coordinate, Year, Updated on, Latitude, Longitude, Location)
File format - text file
Delimiter - "," (use regex while splitting split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1), as there are some fields with comma and enclosed using double quotes.
Output Requirements
Output Fields: crime_type, incident_count
Output File Format: JSON
Delimiter: N/A
Compression: No
Place the output file in the HDFS directory
/user/`whoami`/problem3/solution/
Replace `whoami` with your OS user name
End of Problem
*/

spark-shell --master yarn --conf spark.ui.port=25586 --num-executors 4 --executor-memory 3G --executor-cores 2

sc.setLogLevel("ERROR")

val crimeData = sc.textFile("/public/crime/csv")

val crimeDataDF = crimeData.map(rec => {
  val v = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
  (v(7), v(5))
}).toDF("Location", "crimeType")


val result = crimeDataDF.
                        where($"Location" === "RESIDENCE").
                        groupBy($"crimeType").
                        count().
                        withColumnRenamed("count", "crimeCount").
                        orderBy($"crimeCount".desc).
                        limit(3)

result.coalesce(1).write.mode("overwrite").format("json").save("/user/vivekvarun20/problem3/solution/")

//////////////////////////////////////////////////////////////////

// using Spark_SQL 

val crimeData = sc.textFile("/public/crime/csv")

val crimeDataDF = crimeData.map(rec => {
  val v = rec.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
  (v(7), v(5))
}).toDF("Location", "crimeType")


crimeDataDF.cache

crimeDataDF.registerTempTable("CRIME")

sqlContext.setConf("spark.sql.shuffle.partitions", "1")
val result = sqlContext.sql("select crimeType, count(crimeType) crimeCount from CRIME where Location = 'RESIDENCE' group by crimeType order by crimeCount desc limit 3")

result.coalesce(1).write.mode("overwrite").json("/user/vivekvarun20/problem3/solution/")


//////////////////////////////////////////////////////////////////
