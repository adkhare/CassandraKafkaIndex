package com.bettercloud.cassandra;

import java.io.IOException;

/**
 * Created by amit on 5/28/15.
 */
public interface BetterCloudMBean {

    public static final String MBEAN_NAME = "com.bettercloud.cassandra.BetterCloud:type=Super";

    public String[] allIndexes();

    public String[] indexShards(String indexName);

    public String describeIndex(String indexName) throws IOException;

    public long indexSize(String indexName);

    public long indexLiveSize(String indexName);

    public long writeGeneration();

    public long readGeneration();


}

