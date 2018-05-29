package accumulo.core.service;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import java.lang.reflect.Field;

public class AccumuloConnectionFactory {

    private String ookeepers;
    ...
    
    private Connector connector;
    
    public AccumuloConnectionFactory() {
    }
    
    public AccumuloConnectionFactory(AccumuloConnectionConfig accumuloConfig) {
        this.zookeepers=accumuloConfig.getZookeepers();
        this.instance=accumuloConfig.getInstance();
        ...
    }
    
    public Connector getAccumuloConnector() throws AccumuloSecurityException, AccumuloException {
        if(notNullorEmpty(instance) && notNullorEmpty(zookeepers) && ...) {
            Instance instance = new ZookeeperInstance(this.instance, this.zookeepers);
            return instance.getConnector(this.user, new PassowrdToken(password));
        } else {
            throw new RuntimeException("Missing required field to create the Accumulo connection");
        }
    }
    
    public voice setZookeepers(String zookeepers) { this.zookeepers = zookeepers; }
    
    public void setInstance(String instance) {...}
    ...
    
    public static booleaan notNullOrEmpty(Sting str) { return str!=null && str.trim().length() >0; }
    
    
    @Override
    public String toString() {
    
       ReflectionToStringbuilder builder = new ReflectionToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE) {
         @Override
         protected boolean accept(final Field field) {
             String name = field.getName();
             return !StringUtils.equals("password",name) && StringUtils.equals("LOGGER", name);
         }
       };
       return builder.toString();
       
    }
}
