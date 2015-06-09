package com.bettercloud.cassandra;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.index.PerRowSecondaryIndex;
import org.apache.cassandra.db.index.SecondaryIndexSearcher;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Created by amit on 2/24/15.
 */
public class CassandraIndex extends PerRowSecondaryIndex{

    protected static final Logger logger = LoggerFactory.getLogger(CassandraIndex.class);
    protected String indexName;
    protected ColumnDefinition columnDefinition;
    protected volatile long latest;
    protected RowIndexSupport rowIndexSupport;

    @Override
    public void init() {
        assert baseCfs != null;
        assert columnDefs != null;
        assert columnDefs.size() > 0;
        for(ColumnDefinition columnDefinition1 : columnDefs){
            logger.info(columnDefinition1.getIndexName());
        }
        columnDefinition = columnDefs.iterator().next();
        indexName = columnDefinition.getIndexName();
        rowIndexSupport = new RowIndexSupport(baseCfs,indexName);
        BetterCloud.getInstance().register(rowIndexSupport);
        logger.warn("Creating new RowIndex for {}", indexName);
    }

    @Override
    public void index(ByteBuffer rowKey, ColumnFamily cf) {
        latest = BetterCloud.getInstance().publish(rowKey, cf);
    }

    @Override
    public void delete(DecoratedKey key, OpOrder.Group opGroup) {
    }

    @Override
    public void reload() {
        if(columnDefinition.getIndexOptions() != null && !columnDefinition.getIndexOptions().isEmpty()){
            init();
        }
    }

    @Override
    public void validateOptions() throws ConfigurationException {
        assert columnDefs != null && columnDefs.size() == 1;
    }

    @Override
    public String getIndexName() {
        assert indexName != null;
        return indexName;
    }

    @Override
    protected SecondaryIndexSearcher createSecondaryIndexSearcher(Set<ByteBuffer> columns) {
        return null;
    }

    @Override
    public void forceBlockingFlush() {

    }

    @Override
    public ColumnFamilyStore getIndexCfs() {
        return null;
    }

    @Override
    public void removeIndex(ByteBuffer columnName) {
        setIndexRemoved();
    }

    @Override
    public void invalidate() {

    }

    @Override
    public void truncateBlocking(long truncatedAt) {

    }

    @Override
    public boolean indexes(CellName name) {
        return false;
    }

    @Override
    public long estimateResultRows() {
        return 0;
    }

    @Override
    public String toString() {
        return "RowIndex [index=" + indexName + ", keyspace=" + baseCfs.metadata.ksName + ", table=" + baseCfs.name + ", column=" + columnDefinition.name.toString() + "]";
    }
}
