
package com.bettercloud.cassandra;

import org.apache.cassandra.db.ColumnFamily;
import org.mapdb.Atomic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IndexingService {
    protected static final Logger logger = LoggerFactory.getLogger(BetterCloud.class);
    ExecutorService executorService;
    Map<String, RowIndexSupport> support;
    Atomic.Long reads;

    public IndexingService(Atomic.Long reads) {
        support = new HashMap<>();
        this.reads = reads;
        executorService = Executors.newFixedThreadPool(1);
    }

    public void register(RowIndexSupport rowIndexSupport) {
        this.support.put(rowIndexSupport.baseCfs.metadata.cfName, rowIndexSupport);
    }

    public void index(IndexEntryEvent entryEvent) {
        final ByteBuffer rowkeyBuffer = entryEvent.rowKey;
        final ColumnFamily columnFamily = entryEvent.columnFamily;
        final IndexEntryEvent.Type type = entryEvent.type;
        final RowIndexSupport rowIndexSupport = this.support.get(columnFamily.metadata().cfName);
        try {
            rowIndexSupport.indexRow(rowkeyBuffer, columnFamily);
        } catch (Exception e) {
            logger.error("Error occurred while indexing row of [" + columnFamily.metadata().cfName + "]", e);
            BetterCloud.getInstance().publish(rowkeyBuffer,columnFamily);
        }

        long readGen = reads.incrementAndGet();
        if (logger.isDebugEnabled())
            logger.debug("Read gen:" + readGen);
        /*executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    rowIndexSupport.indexRow(rowkeyBuffer, columnFamily);
                } catch (Exception e) {
                    logger.error("Error occurred while indexing row of [" + columnFamily.metadata().cfName + "]", e);
                    BetterCloud.getInstance().publish(rowkeyBuffer,columnFamily);
                }

                long readGen = reads.incrementAndGet();
                if (logger.isDebugEnabled())
                    logger.debug("Read gen:" + readGen);
            }
        });*/
    }
}
