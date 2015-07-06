/*
 * Copyright 2014, Tuplejump Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.bettercloud.cassandra;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RowIndexSupport {
    protected static final Logger logger = LoggerFactory.getLogger(RowIndexSupport.class);
    private KafkaProducer kafkaProducer;
    private RedisConnection redisConnection;
    private ObjectMapper mapper;
    private CassandraRowAssembler rowAssembler;
    private MessageDTO messageDTO;
    private static final int MINS = 60;
    protected ColumnFamilyStore baseCfs;
    protected String indexName;

    public RowIndexSupport(ColumnFamilyStore baseCfs, String indexName) {
        this.baseCfs = baseCfs;
        this.indexName = indexName;
    }

    public void indexRow(ByteBuffer rowKey, ColumnFamily cf) throws Exception{
        boolean deDupleFlag = true;
        rowAssembler = new CassandraRowAssembler(baseCfs, rowKey, cf);
        mapper = new ObjectMapper();
        //rowAssembler.assemble();
        messageDTO = rowAssembler.getMessageDTO();
        List<Exception> exceptions = new ArrayList<Exception>();

        if(messageDTO != null){
            try{
                if(logger.isDebugEnabled()){
                    logger.debug("Deduping key - "+messageDTO.getTimestamp()+"-"+indexName+"-"+messageDTO.getKeys() + "using Redis Cluster");
                }
                deDupleFlag = dedupeRedis(messageDTO.getTimestamp()+"-"+indexName+"-"+messageDTO.getKeys(),"");
            }catch (Exception e){
                logger.warn("Error with Redis Dedupe - "+e.getMessage());
                //exceptions.add(e);
                //throw new BetterCloudIndexException(messageDTO.getTimestamp(),getMessageJson(messageDTO),e.getMessage(),rowKey,cf);
            }
            if(deDupleFlag) {
                try{
                    if(logger.isDebugEnabled()){
                        logger.debug("Row being set to kafka - "+getMessageJson(messageDTO));
                    }
                    queueKafkaMessage(getMessageJson(messageDTO));
                }catch (Exception e){
                    removeDedupe(messageDTO.getTimestamp());
                    exceptions.add(e);
                    //throw new BetterCloudIndexException(messageDTO.getTimestamp(),getMessageJson(messageDTO),e.getMessage(),rowKey,cf);
                }
            }
        }else {
            throw new BetterCloudIndexException("","","MessageDTO is null",rowKey,cf,true);
        }
        if(!exceptions.isEmpty()){
            StringBuilder sb = new StringBuilder();
            for(Exception e : exceptions){
                sb.append(e.getMessage());
            }
            throw new BetterCloudIndexException(messageDTO.getTimestamp(),getMessageJson(messageDTO),sb.toString(),rowKey,cf,true);
        }
    }

    private boolean dedupeRedis(String key, String value){
        boolean returnFlag = true;
        redisConnection = new RedisConnection(BetterCloud.getInstance().getRedisClusterConnectionFromPool());
        returnFlag = redisConnection.setKey(key, value, MINS*60);
        return returnFlag;
    }

    public String queueKafkaMessage(String msg){
        String returnVal = "E";
        kafkaProducer = new KafkaProducer("10.0.0.5:9092");
        returnVal = kafkaProducer.produce(messageDTO.getKeyspace()+"."+messageDTO.getEntity(),msg,"test-cassandra-kafka-1");
        return returnVal;
    }

    private String getMessageJson(MessageDTO msg){
        String returnVal = "";
        try {
            returnVal = mapper.writeValueAsString(msg);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return returnVal;
    }

    private void removeDedupe(String key){
        redisConnection = new RedisConnection(BetterCloud.getInstance().getRedisClusterConnectionFromPool());
        redisConnection.deleteKey(key);
    }


}
