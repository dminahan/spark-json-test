package dminahan.spark.json;

//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
//import static org.apache.spark.sql.functions.explode;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FakeJsonMain implements Serializable {

   public JavaSparkContext context;
   public SQLContext sqlContext=null;
   
   private static final Logger LOGGER=LoggerFactory.getLogger(JsonMain.class);
 
public static void main(String args) {
   JsonMain jsonMain= new JsonMain();
   jsonMain.init();
   jsonMain.run(jsonMain.sqlContext.sparkSession());
}

public void init() {
   String master="local[*]";
   SparkConf conf =new SparkConf()
                .setAppName(JsonMain.class.getName())
                .setMaster(master);
    
   context=new JavaSparkContext(conf);
   sqlContext=new SQLContext(context);
}
   
public void run(SparkSession sparkSession) {
   Dataset<Row> records = sparkSession.read().json("fakeJson.json");
   records.printSchema();
   records.show(false);
   
   records.createOrReplaceTempView("fake_records");
   
   //TODO: When best to take the substring of the source "foo:" and test if the UUID is not null/empty?
   Dataset<Row> members=records.filter(col("state").isNotNull().and(col("updated").isNotNull())
                               .and(col("user").isNotNull()).and(col("originator").isNotNull())
                               .and(col("originator").startsWith("foo"))
   )
   .select(
      col("state"),
      col("updated"),
      col("user"),
      col("originator)
   )
   .toDF("state","updated","user","originator");
      
   members.createOrReplaceView("fake");
      
      //Execute a sample SQL (note that the results could be written to HDFS if desired)
      Dataset<Row> sampleSqlResults=sparkSession.sql(
         "select state" +
         ", updated at updatedTime" +
         ", user" +
         ", originator as source" +
         ", originator as uuid" +
         " from fake " +
         " where originator is not null"
      );
      
      sampleSqlResults.printSchema();
      sampleSqlResults.show(10,false);
   /*  Multi-line example in scala
   val mdf = spark.read.option("multiline", "true").json("multi.json")
   mdf.show(false)
   */

}
 
}
