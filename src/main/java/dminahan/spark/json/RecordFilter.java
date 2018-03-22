//package dminahan.spark.json.filters;
package dminahan.spark.json;

//import dminahan.spark.json.models.JsonRecord;
import org.apache.commons.lang.StringUtils;

import java.util.function.Predicate;

/**
 * Class to wrap some of the filtering logic into Predicate filters on raw records for 
 * complete processing in Spark
 */
public class RecordFilter {

   public static boolean filterInvalidRecord(JsonRecord jsonRecord) {
       Predicate<JsonRecord> conditions=e -> StringUtils.isNotBlank(e.getUser());
       conditions=conditions.and(e->StringUtils.isNotBlank(e.getState()));
       conditions=conditions.and(e->StringUtils.isNotBlank(e.getUpdated()));
       //Correct Originator filter needed
       //conditions=conditions.and(e->StringUtils.isNotBlank(e.getOriginator())&&e.getOriginator().startsWith("foo"));
       conditions=conditions.and(e->StringUtils.isNotBlank(getOriginator()));
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
   
}
