package dminahan.spark.json;

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
   Dataset<Row> df = sparkSession.read().json("fakeJson.json");
   df.printSchema();
   df.show(false);
   /*  Multi-line example in scala
   val mdf = spark.read.option("multiline", "true").json("multi.json")
   mdf.show(false)
   */

}
 
}
