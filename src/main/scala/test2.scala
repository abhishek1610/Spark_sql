//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
//import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}

import org.apache.spark.{SparkConf, SparkContext}
import java.nio.file.{Paths, Path}
import com.databricks.spark.avro._
//import org.apache.spark.sql.hive

object test2 {



  def main(args: Array[String]) {
    //Initiate spark context with spark master URL. You can modify the URL per your environment.

   val conf1= new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
    val sc = new SparkContext(conf1)

    val sqlContext1 = new org.apache.spark.sql.hive.HiveContext(sc)

   // val path1 = Paths.get("C:\\Users\\abhishek\\workspace\\test12\\inp.txt")

   // val delim=args[0]

    //sqlContext1.sql("CREATE external TABLE IF NOT EXISTS sample (key INT,name String) ROW FORMAT DELIMITED FIELDS TERMINATED BY 'delim' STORED AS TEXTFILE location 'file:///C:/Users/abhishek/test'")

  //import sqlContext1._

    sqlContext1.sql("CREATE  TABLE IF NOT EXISTS sample (key INT,name String) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" )
     sqlContext1.sql("LOAD DATA LOCAL INPATH 'file:///C:/Users/abhishek/workspace/test12/inp.txt' INTO TABLE sample")

    // Queries are expressed in HiveQL
   sqlContext1.sql("select * from sample").collect().foreach(println)
    //saving as avro format using dataframe

    val df = sqlContext1.sql("select * from sample")
    df.save("test.avro","com.databricks.spark.avro")

    val df1  = sqlContext1.load("test.avro", "com.databricks.spark.avro")
    df.save("test1.parquet", "parquet")


  }
}
