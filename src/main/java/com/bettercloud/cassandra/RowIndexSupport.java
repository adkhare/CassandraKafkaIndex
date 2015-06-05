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

/**
 * User: satya
 * Interface for writing a row to a lucene index.
 */
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

    public void indexRow(ByteBuffer rowKey, ColumnFamily cf) {
        rowAssembler = new CassandraRowAssembler();
        kafkaProducer = new KafkaProducer();
        mapper = new ObjectMapper();
        redisConnection = new RedisConnection();
        rowAssembler.init(baseCfs, rowKey, cf);
        rowAssembler.assemble();
        messageDTO = rowAssembler.getMessageDTO();

        try {
            if(dedupeRedis(messageDTO.getTimestamp(),"")) {
                queueKafkaMessage(getMessageJson(messageDTO));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }

    private boolean dedupeRedis(String key, String value){
        redisConnection.init(BetterCloud.getInstance().getRedisClusterConnectionFromPool());
        return redisConnection.setKey(key, value, MINS*60);
    }

    public String queueKafkaMessage(String msg){
        String returnVal = "";
        try{
            kafkaProducer.init("10.0.0.5:9092");
            returnVal = kafkaProducer.produce(messageDTO.getKeyspace()+"."+messageDTO.getEntity(),msg,"test-cassandra-kafka-1");
            logger.info("Row sent to Kafka - " + msg);
        }catch(Exception e){
            logger.warn(e.getMessage());
        }
        return returnVal;
    }

    private String getMessageJson(MessageDTO msg){
        String returnVal = "";
        try {
            returnVal = mapper.writeValueAsString(msg);
        } catch (IOException e) {
            logger.error(e.getLocalizedMessage());
            e.printStackTrace();
        }
        return returnVal;
    }



}
