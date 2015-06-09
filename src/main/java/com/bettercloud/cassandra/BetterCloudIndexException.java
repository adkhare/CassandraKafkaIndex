package com.bettercloud.cassandra;

import org.apache.cassandra.db.ColumnFamily;

import java.nio.ByteBuffer;

/**
 * Created by amit on 6/8/15.
 */
public class BetterCloudIndexException extends Exception {
    private String messageKey;
    private String messageJSON;
    private String message;
    private ByteBuffer rowKey;
    private ColumnFamily cf;

    public BetterCloudIndexException(String messageKey,String messageJSON, String message,ByteBuffer rowKey,ColumnFamily cf){
        this.messageKey = messageKey;
        this.messageJSON = messageJSON;
        this.message = messageJSON + message;
        this.rowKey = rowKey;
        this.cf = cf;
    }

    public String getMessageKey(){
        return this.messageKey;
    }

    public String getMessage(){
        return this.message;
    }

    public ByteBuffer getRowKey(){
        return this.rowKey;
    }

    public ColumnFamily getCf(){
        return this.cf;
    }
}
