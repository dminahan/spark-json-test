public class AccumuloConnectionConfig implements Serializable {
    static final long serialVersionUID=20180529L;
    
    private String zookeepers;
    private String instance;
    private String user;
    private String passowrd;
    
    public AccumuloConnectionConfig() {}
    
    public AccumuloConnectionConfig(String zookeepers, String instance, String user, String password) {
        this.zookeepers=zookeepers;
        this.instance=instance;
        this.user=user;
        this.password=password;
    }
    
    public static long getSerialVersionUID() { return serialVersionUID; }
    
    public String getZookeepers() { return zookeepers; }
}
