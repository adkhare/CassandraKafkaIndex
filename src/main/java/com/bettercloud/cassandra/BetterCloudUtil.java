package com.bettercloud.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.StringTokenizer;


/**
 * Created by amit on 2/26/15.
 */
public class BetterCloudUtil {

    public static final String UTF8Validator = "org.apache.cassandra.db.marshal.UTF8Type";
    public static final String Int32Validator = "org.apache.cassandra.db.marshal.Int32Type";
    public static final String LongValidator = "org.apache.cassandra.db.marshal.LongType";
    public static final String DoubleValidator = "org.apache.cassandra.db.marshal.DoubleType";
    public static final String FloatValidator = "org.apache.cassandra.db.marshal.FloatType";
    public static final String UUIDValidator = "org.apache.cassandra.db.marshal.UUIDType";
    public static final String TimeUUIDValidator = "org.apache.cassandra.db.marshal.TimeUUIDType";
    public static final String TimestampValidator = "org.apache.cassandra.db.marshal.TimestampType";
    public static final String BooleanValidator = "org.apache.cassandra.db.marshal.BooleanType";
    public static final String partKeyType = "PARTITION_KEY";
    public static final String clusKeyType = "CLUSTERING_KEY";
    public static final String regularType = "REGULAR";

    public static String[] getDataDirs() throws IOException, ConfigurationException {
        return DatabaseDescriptor.getAllDataFileLocations();
    }

    public static int toInt(ByteBuffer col){
        return ByteBufferUtil.toInt(col);
    }

    public static String toString(ByteBuffer col) {
        ByteBuffer bb = ByteBufferUtil.clone(col);
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return new String(chars(bytes));
    }

    private static char[] chars(byte[] bytes)
    {
        char[] chars = new char[bytes.length];
        for (int i = 0; i < bytes.length; i++)
        {
            int pos = bytes[i] & 0xff;
            chars[i] = (char) pos;
        }
        return chars;
    }

    public static boolean isEmpty(ByteBuffer byteBuffer)
    {
        return byteBuffer.remaining() == 0;
    }

    public static String reorderTimeUUId(String originalTimeUUID) {
        StringTokenizer tokens = new StringTokenizer(originalTimeUUID, "-");
        if (tokens.countTokens() == 5) {
            String time_low = tokens.nextToken();
            String time_mid = tokens.nextToken();
            String time_high_and_version = tokens.nextToken();
            String variant_and_sequence = tokens.nextToken();
            String node = tokens.nextToken();
            return time_high_and_version + '-' + time_mid + '-' + time_low + '-' + variant_and_sequence + '-' + node;
        }

        return originalTimeUUID;
    }

    public static String getString(ByteBuffer colValue, AbstractType type){
        switch ((CQL3Type.Native)type.asCQL3Type()){
            case TEXT :
                return type.getString(colValue);
            case ASCII :
                return type.getString(colValue);
            case VARCHAR :
                return type.getString(colValue);
            case INT :
                return ((Integer) type.compose(colValue)).toString()+"";
            case BIGINT :
                return ((Number) type.compose(colValue)).longValue()+"";
            case VARINT :
                return ""+((Number) type.compose(colValue)).longValue();
            case COUNTER :
                return ""+((Number) type.compose(colValue)).longValue();
            case DECIMAL :
                return ""+((Number) type.compose(colValue)).doubleValue();
            case DOUBLE :
                return ""+((Number) type.compose(colValue)).doubleValue();
            case FLOAT :
                return ""+((Number) type.compose(colValue)).floatValue();
            case UUID :
                return type.getString(colValue);
            case TIMEUUID :
                return BetterCloudUtil.reorderTimeUUId(type.getString(colValue));
            case TIMESTAMP :
                return type.getString(colValue);
            case BOOLEAN :
                return ((Boolean) type.compose(colValue)).toString();
            default :
                return type.getString(colValue);
        }
    }
}
