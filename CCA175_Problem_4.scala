/*Problem4.scala

Instructions
Convert NYSE data into parquet

NYSE data Description
Data is available in local file system under /data/NYSE (ls -ltr /data/NYSE)

NYSE Data information:

Fields (stockticker:string, transactiondate:string, openprice:float, highprice:float, lowprice:float, closeprice:float, volume:bigint)
Output Requirements
Column Names: stockticker, transactiondate, openprice, highprice, lowprice, closeprice, volume
Convert file format to parquet
Place the output file in the HDFS directory
/user/`whoami`/problem4/solution/
Replace `whoami` with your OS user name
End of Problem


*/

// hdfs dfs -copyFromLocal /data/nyse/ data/

import scala.io.Source._
import java.io.File



val nyse_data = sc.textFile("data/nyse/")

val nyseDF = nyse_data.map( rec => {
  val v = rec.split(',')
  (v(0),v(1),v(2).toFloat,v(3).toFloat,v(4).toFloat,v(5).toFloat,v(6).toInt)
}).toDF("stockticker", "transactiondate", "openprice", "highprice", "lowprice", "closeprice", "volume") 


nyseDF.write.parquet("/user/vivekvarun20/problem4/solution/")



