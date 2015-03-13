package com.bettercloud.cassandra;

import java.util.*;

/**
 * Created by amit on 3/2/15.
 */
public class MessageDTO {

    private String keyspace;

    private String entity;

    private String operation;

    private Map<String,String> keys;

    private Map<String,String> object;

    private List<Map<String,String>> collectionObject;

    MessageDTO(){
        keyspace = "";
        entity = "";
        operation = "";
        keys = new HashMap<String,String>();
        object = new HashMap<String,String>();
        collectionObject = new ArrayList<Map<String,String>>();
    }

    public void setKeyspace(String keyspace){
        this.keyspace = keyspace;
    }

    public void setEntity(String entity){
        this.entity = entity;
    }

    public void setKeys(String keyName, String keyValue){
        this.keys.put(keyName,keyValue);
    }

    public void setObject(String columnName, String columnValue){
        this.object.put(columnName,columnValue);
    }

    protected void setCollectionObject(Map<String,String> collectionObjParam){
        this.collectionObject.add(collectionObjParam);
    }

    public void setOperation(String operation){
        this.operation = operation;
    }

    public String getKeyspace(){
        return this.keyspace;
    }

    public String getEntity(){
        return this.entity;
    }

    public String getKeys(){
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String,String>> it =  keys.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String,String> entry = it.next();
            sb.append("'"+entry.getKey()+"':'"+entry.getValue()+"',");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public String getObject(){
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String,String>> it =  object.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String,String> entry = it.next();
            sb.append("'"+entry.getKey()+"':'"+entry.getValue()+"',");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public String getCollectionObject(){
        StringBuilder sb = new StringBuilder();
        for(Map<String,String> map : collectionObject){
            Iterator<Map.Entry<String,String>> it =  map.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry<String,String> entry = it.next();
                sb.append("'"+entry.getKey()+"':'"+entry.getValue()+"',");
            }
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public String getOperation(){
        return operation;
    }

    @Override
    public String toString()
    {
        return "Entity[entity="+entity+"]";
    }

}
