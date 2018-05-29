package accumulo.core.spark;

public abstract class AccumuloRelation extends BaseRelation  implements TableScan, Serializable {
  public static final String ZOOKEEPERS="zookeepers";
  ...
  
  public static final String ROOT_VIS="root";
  public static final String TYPE_DELIMITER=":";
  public static final String VIS_DELIN="|";
  
  protected final SQLContext sqlContext;
  protected final scala.collection.immutable.Map<String, String> options;
  
  public AccumuloRelation(SQLContext sqlContext, scala.collection.immutable.Map<String, String> options) {
    this.sqlContext=sqlContext;
    this.options=options;
    
    validateOptions(this.options);
  }
  
  abstract protected boolean validateChildOptions(scala.collection.immutable.Map<String, String> options);
  
  abstract protected void configureJob(Job job);
  
  abstract protected RDD<Row> convertToRowDataSet(JavaPairRDD<Key, Value> rdd);
  
  abstract protected void writeRecord(Row row, BatchWriter writer);
  
  @Override
  public SQLContext sqlContext() (return this.sqlContext);
  
  protected boolean validateOptions(scala.collction.immutable.Map<String, String> options) {
    validateOption(options, INSTANCE);
    ...
    
    return validateChildOptions(options);
  }
  
  private String validateOption(scala.collection.immutable.Map<String, String> options, String optionName) {
  
     Option<String> option = options.get(optionName);
     
     if(option == null|| option.isEmpty()) {
       throw new IllegalArgumentException("invalid option " + optionName + "; it must be provided");
     }
     return option.get();
  }
  
  @Override
  public RDD<Row> buildScan() {
  
     Job job = null;
     try {
         job = Job.getInstance();
     } catch(IOException e) {
         throw new RuntimeException("Error while trying to setup job to scan accumulo", e);
     }
     try {
         AbstractInputFormat.setConnectorInfo(job, options.get(USER).get(), new PasswordToken(options.get(PASSWORD).get()));
     } catch (AccumuloSecurityException e) {
     throw new RuntimeException("Error while tryin to set asuthorization to scan accumulo", e);
     }
     
     AbstractInputFormat.setScanAuthorizations
  }
}
