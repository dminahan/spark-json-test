package dminahan.spark.json;

import org.apache.spark.sql.SparkSession;

/**
 * Main runnable spark class
 */
public class App {

   public static void main(String[] args) {
      SparkSession spark = SparkSession.builder().appName("Json Test").getOrCreate();
      JsonMain jsonMain=new JsonMain();
      jsonMain.run(spark);
   }
}
