package com.bettercloud.cassandra;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by amit on 3/2/15.
 */
public class KafkaProducer {

    private Properties props;
    private ProducerConfig config;
    private Producer<String,String> producer;
    public KafkaProducer(){
        props = new Properties();
    }

    public void init(String brokerList){
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");

        config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
    }

    public String produce(String key, String message,String topic){

        String returnString = "";
        try{
            KeyedMessage<String,String> msg = new KeyedMessage<String, String>(topic,key,message);
            producer.send(msg);
            returnString = "S";
        }catch (Exception e){
            returnString = e.getStackTrace().toString();
        }

        return returnString;
    }
}
