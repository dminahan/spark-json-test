package dminahan.spark.json.filters;

import dminahan.spark.json.models.JsonRecord;
import org.apache.commons.lang.StringUtils;
//import org.apache.commons.lang3.StringUtils;

import java.util.function.Predicate;
import java.util.Arrays;

/**
 * Class to wrap some of the filtering logic into Predicate filters on raw records for 
 * complete processing in Spark
 */
public class RecordFilter {
   public static String[] FAKE_GOOD_LIST=new String[]{
      "WORKING",
      "GOOD",
      "USEFUL"
   };
   
   public static String[] FAKE_BAD_LIST=new String[]{
      "BAD",
      "DROP",
      "NOTUSEFUL"        
   };

   public static boolean filterInvalidRecord(JsonRecord jsonRecord) {
       Predicate<JsonRecord> conditions=e -> StringUtils.isNotBlank(e.getUser());
       conditions=conditions.and(e->StringUtils.isNotBlank(e.getState()));
       conditions=conditions.and(e->StringUtils.isNotBlank(e.getUpdated()));
       //Correct Originator filter needed
       //conditions=conditions.and(e->StringUtils.isNotBlank(e.getOriginator())&&e.getOriginator().startsWith("foo"));
       conditions=conditions.and(e->StringUtils.isNotBlank(e.getSystem()));
       return RecordFilter.isValidRecord(conditions, jsonRecord);
   }
   
   /**
    * Verifies the record is not null a well as meets the specified Predicates.  You can chain
    * Predicates together by calling and(), or(), negate(), etc.  And you can test the record against 
    * the Predicate by calling predicate.test(record).
    * @param condition
    * @param <T> record
    * @return {@code true} If the given record is valid based on the Predicate(s) passed in.
    */
    public static<T> boolean isValidRecord(Predicate<T> condition, T record) {
        return record!=null && condition.test(record);
    }
   
   public static boolean filterGoodRecords(JsonRecord record) {
      Predicate<JsonRecord> conditions=e->inGoodStateList(e.getState());
      return isValidRecord(conditions,record);
   }
   
   public static boolean filterBadRecords(JsonRecord record) {
      Predicate<JsonRecord> conditions=e->inBadStateList(e.getState());
      return isValidRecord(conditions,record);
   }
   
   public static boolean inGoodStateList(String state) {
      //TODO:  Look to rework maybe for contains vs current approach
      int index=-1;
      if(state!=null){
         index=Arrays.binarySearch(FAKE_GOOD_LIST, state);
      }
      return index >-1;
   }
   
   public static boolean inBadStateList(String state) {
      //TODO:  Look to rework maybe
      int index=-1;
      if(state!=null){
         index=Arrays.binarySearch(FAKE_BAD_LIST,state);
      }
      return index>-1;
   }
   
}
