package com.bettercloud.cassandra;

/**
 * Created by amit on 5/28/15.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

class IndexEventSubscriber extends Thread {
    protected static final Logger logger = LoggerFactory.getLogger(BetterCloud.class);
    BlockingQueue<IndexEntryEvent> queue;
    AtomicBoolean stopped;
    AtomicBoolean started;
    IndexingService indexingService;


    public IndexEventSubscriber(IndexingService indexingService, BlockingQueue<IndexEntryEvent> queue) {
        this.indexingService = indexingService;
        this.queue = queue;
        stopped = new AtomicBoolean(false);
        started = new AtomicBoolean(false);
        setName("BetterCloud Index Entry Subscriber");
    }


    @Override
    public synchronized void start() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stopped.set(true);
            }
        });
        super.start();
        started.set(true);
        BetterCloud.logger.warn("********* Indexer Thread Started ************");
    }

    @Override
    public void run() {
        while (!stopped.get()) {
            try {
                if (queue.peek() != null) {
                    IndexEntryEvent indexEntryEvent = queue.remove();
                    indexingService.index(indexEntryEvent);
                }
                Thread.yield();
            } catch (Exception e) {
                logger.error("Error occurred while indexing row", e);
            }
        }

    }

}
