package dminahan.spark.json.models;

public class JsonFeedbackRecord {
    protected String user=null;
    protected String state=null;
    protected String updated=null;
    //protected String originator=null;
    protected String uuid=null;
    protected String system=null;
    protected Long timestamp=-1;
    protected int feedback=-1;
    
    public String getUser() {
        return this.user;
    }
    
    public void setUser(String user) {
        this.user=user;
    }
    
    public String getState() {
        return this.state;
    }
    
    public void setState(String state) {
        this.state=state;
    }
    
    public String getUpdated() {
        return this.updated;
    }
    
    public void setUpdated(String updated) {
        this.updated=updated;
    }
    
    /*
    public String getOriginator() {
        return this.originator;
    }
    
    public void setOriginator(String originator) {
        this.originator=originator;
    }
    */
    
    public String getUuid(){
       return this.uuid;
    }
    
    public void setUuid(String uuid) {
       this.uuid=uuid;
    }
    
    public String getSystem() {
       return this.system;
    }
    
    public void setSystem(String system) {
       this.system=system;
    }
    
    public Long getTimestamp() {
       return this.timestamp;
    }
    
    public void setTimestamp(Long timestamp) {
       this.timestamp=timestamp;
    }   
    public int getFeedback() {
       return this.feedback;
    }
    
    public void setFeedback(int feedback) {
       this.feedback=feedback;
    }       
}
