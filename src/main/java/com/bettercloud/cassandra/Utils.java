package com.bettercloud.cassandra;

/**
 * Created by amit on 5/28/15.
 */

import org.slf4j.Logger;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * User: satya
 */
public class Utils {

    public static SimpleTimer getStartedTimer(Logger logger) {
        SimpleTimer timer = new SimpleTimer(logger);
        timer.start();
        return timer;
    }

    public static class SimpleTimer {
        long startTime;
        long endTime;
        Logger logger;

        SimpleTimer(Logger logger) {
            this.logger = logger;
        }

        SimpleTimer() {
        }

        public void start() {
            startTime = System.nanoTime();
        }

        public void end() {
            endTime = System.nanoTime();
        }

        public double time() {
            return timeNano() / 1000000;
        }

        public long timeNano() {
            return endTime - startTime;
        }

        public void logTime(String prefix) {
            if (logger != null && logger.isWarnEnabled())
                logger.warn(String.format("{} - took [{}] milli seconds"), prefix, time());
        }

        public void endLogTime(String prefix) {
            end();
            logTime(prefix);
        }

        public double endGetTime() {
            end();
            return time();
        }

        public long endGetTimeNano() {
            end();
            return timeNano();
        }

    }

    public static String stringify(ByteBuffer byteBuffer) {
        return javax.xml.bind.DatatypeConverter.printBase64Binary(getBytes(byteBuffer));
    }

    public static byte[] getBytes(ByteBuffer bb) {
        byte[] b = new byte[bb.remaining()];
        bb.get(b);
        return b;
    }
    public static void write(ByteBuffer buffer, DataOutput out) throws IOException
    {
        if (buffer.hasArray())
        {
            out.write(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        }
        else
        {
            for (int i = buffer.position(); i < buffer.limit(); i++)
            {
                out.writeByte(buffer.get(i));
            }
        }
    }


}
