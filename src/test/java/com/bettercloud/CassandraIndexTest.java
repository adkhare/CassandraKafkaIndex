package com.bettercloud;

import com.bettercloud.cassandra.KafkaProducer;
import com.bettercloud.util.CQLUnitD;
import junit.framework.Assert;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ShardedJedisPool;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by amit on 3/5/15.
 */
public class CassandraIndexTest extends CassandraIndexTestBase {
    private String keyspace = "dummytest";

    public CassandraIndexTest() {
        cassandraCQLUnit = CQLUnitD.getCQLUnit(null);
    }
    //@Test
    public void createIndexAndInsert(){
        try{
            createKS(keyspace);
            createTableAndIndexForRow();
            Assert.assertTrue(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            dropKS(keyspace);
        }
    }

    //@Test
    public void createIndexAndInsertUpdate(){
        try{
            createKS(keyspace);
            createTableAndIndexForRow();
            updateIndex();
            Assert.assertTrue(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            dropKS(keyspace);
        }
    }

    //@Test
    public void createIndexAndInsertDelete(){
        try{
            createKS(keyspace);
            createTableAndIndexForRow();
            deleteIndex();
            Assert.assertTrue(true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            dropKS(keyspace);
        }

    }

    private void createTableAndIndexForRow() throws InterruptedException {
        getSession().execute("USE " + keyspace + ";");
        getSession().execute("CREATE TABLE TAG2(key int, tags text, state varchar, segment int, magic text, PRIMARY KEY(segment, key))");
        int i = 0;
        while (i < 40) {
            if (i == 20) {
                getSession().execute("CREATE CUSTOM INDEX tagsandstate ON TAG2(magic) USING 'com.bettercloud.cassandra.CassandraIndex'");
            }
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 1) + ",'hello1 tag1 lol1', 'CA'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 2) + ",'hello1 tag1 lol2', 'LA'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 3) + ",'hello1 tag2 lol1', 'NY'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 4) + ",'hello1 tag2 lol2', 'TX'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 5) + ",'hllo3 tag3 lol3',  'TX'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 6) + ",'hello2 tag1 lol1', 'CA'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 7) + ",'hello2 tag1 lol2', 'NY'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 8) + ",'hello2 tag2 lol1', 'CA'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 9) + ",'hello2 tag2 lol2', 'TX'," + i + ")");
            getSession().execute("insert into " + keyspace + ".TAG2 (key,tags,state,segment) values (" + (i + 10) + ",'hllo3 tag3 lol3', 'TX'," + i + ")");
            i = i + 10;
        }
    }

    private void updateIndex() throws InterruptedException {
        getSession().execute("USE "+keyspace+";");
        int i = 0;
        while (i < 40) {
            getSession().execute("update " + keyspace + ".TAG2 set state = 'TX' where key = " + (i + 1) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'GA' where key = " + (i + 2) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'MI' where key = " + (i + 3) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'WA' where key = " + (i + 4) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'CA' where key = " + (i + 5) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'NY' where key = " + (i + 6) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'FL' where key = " + (i + 7) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'AK' where key = " + (i + 8) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'AK' where key = " + (i + 9) + " and segment = "+i);
            getSession().execute("update " + keyspace + ".TAG2 set state = 'GA' where key = " + (i + 10) + " and segment = "+i);
            i = i + 10;
        }
    }

    private void deleteIndex() throws InterruptedException {
        getSession().execute("USE "+keyspace+";");
        int i = 0;
        while (i < 40) {
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 1) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 2) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 3) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 4) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 5) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 6) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 7) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 8) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 9) + " and segment = "+i);
            getSession().execute("delete from " + keyspace + ".TAG2 where key = " + (i + 10) + " and segment = "+i);
            i = i + 10;
        }
    }

    //@Test
    public void sendKafkaMessage() throws InterruptedException{
        KafkaProducer kafkaProducer = new KafkaProducer("bettercloud-testing.cloudapp.net:9092");
        String key = "testing", msg = "testing";
        //kafkaProducer.init("bettercloud-testing.cloudapp.net:9092");
        org.junit.Assert.assertEquals("Test Successful", "S", kafkaProducer.produce("testing", "testing", "test-cassandra-kafka"));
    }

    //@Test
    public void testRedisConnection() throws InterruptedException{
        JedisPoolConfig jedisPoolConfig;
        ShardedJedisPool jedisPool;
        JedisCluster jedisCluster;
        jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(10);
        jedisPoolConfig.setMaxWaitMillis(1);
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        jedisClusterNodes.add(new HostAndPort("bc-test-red001.cloudapp.net",6379));
        jedisClusterNodes.add(new HostAndPort("bc-test-red002.cloudapp.net",6379));
        jedisClusterNodes.add(new HostAndPort("bc-test-red003.cloudapp.net",6379));
        jedisCluster = new JedisCluster(jedisClusterNodes);
        jedisCluster.setnx("hi", "hi");
        jedisCluster.expire("hi",1);
        Assert.assertEquals("hi",jedisCluster.get("hi"));
    }
}
