package dminahan.spark.json;

//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
//import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.substring_index;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dminahan.spark.json.filters.RecordFilter;
import dminahan.spark.json.models.JsonRecord;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FakeJsonMain implements Serializable {

   /**
	 * 
	 */
	private static final long serialVersionUID = 3582672377205640091L;
public JavaSparkContext context;
   public SQLContext sqlContext=null;
   
   private static final Logger LOGGER=LoggerFactory.getLogger(FakeJsonMain.class);
 
public static void main(String args) {
	FakeJsonMain jsonMain= new FakeJsonMain();
   jsonMain.init();
   jsonMain.run(jsonMain.sqlContext.sparkSession());
}

public void init() {
   String master="local[*]";
   SparkConf conf =new SparkConf()
                .setAppName(FakeJsonMain.class.getName())
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
      col("originator")
   )
   .withColumn("uuid", substring_index(col("originator"), ":", -1))
   .withColumn("system", substring_index(col("originator"), ":", 1))
   .toDF("state","updated","user","originator","uuid","system");
      
   members.createOrReplaceTempView("fake");

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
   
   //Trying to use filters and get typed Dataset
   Dataset<JsonRecord> validRecords=members.as(Encoders.bean(JsonRecord.class)) //convert row to JsonRecord via encoder
      .filter(RecordFilter::filterInvalidRecord);  //filter out invalid json records
   
   validRecords.printSchema();
   validRecords.show(false);
   
   //JsonRecordAggregator aggregator=new JsonRecordAggregator();
   //Dataset<GoodJsonRecord> count

}
 
}
