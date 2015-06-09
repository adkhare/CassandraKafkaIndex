
package com.bettercloud.cassandra;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

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
        if(messageDTO != null){
            try{
                deDupleFlag = dedupeRedis(messageDTO.getTimestamp()+"-"+indexName+"-"+messageDTO.getKeys(),"");
            }catch (Exception e){
                throw new BetterCloudIndexException(messageDTO.getTimestamp(),getMessageJson(messageDTO),e.getMessage(),rowKey,cf);
            }
            if(deDupleFlag) {
                try{
                    queueKafkaMessage(getMessageJson(messageDTO));
                }catch (Exception e){
                    removeDedupe(messageDTO.getTimestamp());
                    throw new BetterCloudIndexException(messageDTO.getTimestamp(),getMessageJson(messageDTO),e.getMessage(),rowKey,cf);
                }
            }
        }else {
            throw new BetterCloudIndexException("","","MessageDTO is null",rowKey,cf);
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
