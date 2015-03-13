package com.bettercloud.cassandra;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CFDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by amit on 3/4/15.
 */
public class CassandraRowAssembler {

    protected static final Logger logger = LoggerFactory.getLogger(CassandraRowAssembler.class);

    private ColumnFamilyStore baseCfs;

    private ByteBuffer rowKey;

    private ColumnFamily cf;

    private MessageDTO messageDTO;

    private static enum Operation {
        delete,
        insert,
        update
    }

    public void init(ColumnFamilyStore baseCfs, ByteBuffer rowKey, ColumnFamily cf){
        this.baseCfs = baseCfs;
        this.rowKey = rowKey;
        this.cf = cf;
        messageDTO = new MessageDTO();
    }

    public MessageDTO getMessageDTO(){
        return messageDTO;
    }

    public void assemble(){
        int partitionKeyCount = 0,clusteringKeyCount = 0;
        String partitionKeyStringPart = "",clusteringKeyStringPart = "";
        String[] partitionKeys, clusteringKeys;
        partitionKeyStringPart = baseCfs.metadata.getKeyValidator().getString(rowKey);
        partitionKeys = partitionKeyStringPart.split(":");
        Iterator<ColumnDefinition> itColDef = cf.metadata().allColumns().iterator();
        String keys = "";
        Iterator<Column> itRegCols = cf.iterator();
        logger.warn("Decorated Key : " + baseCfs.partitioner.decorateKey(rowKey));
        if(itRegCols.hasNext()){
            logger.warn("In Loop");
            keys = getClusteringKeys(rowKey,itRegCols.next());
            itRegCols = cf.iterator();
        }
        clusteringKeyStringPart = StringUtils.replace(keys, baseCfs.metadata.getKeyValidator().getString(rowKey) + ":", "");
        clusteringKeys = clusteringKeyStringPart.split(":");
        logger.warn("Clustering Keys : "+clusteringKeyStringPart);
        while(itColDef.hasNext()){
            ColumnDefinition colDef = itColDef.next();
            switch (colDef.type.toString()) {
                case BetterCloudUtil.partKeyType: {
                    if(partitionKeys.length > 0)
                    assemblePartitionKeys(colDef, partitionKeys, partitionKeyCount);
                }
                break;
                case BetterCloudUtil.clusKeyType: {
                    if(clusteringKeys.length > 0)
                    assembleClusteringKeys(clusteringKeys, clusteringKeyCount);
                }
                break;
                case BetterCloudUtil.regularType : {
                        assembleRegularColumns(itRegCols);
                }
                break;
            }
        }
        messageDTO.setKeyspace(baseCfs.metadata.ksName);
        messageDTO.setEntity(baseCfs.name);
        if(cf.isMarkedForDelete()){
            messageDTO.setOperation(Operation.delete.name());
        }else{
            messageDTO.setOperation(Operation.insert.name());
        }
        logger.warn("Operation : " + messageDTO.getOperation());
    }

    private void assemblePartitionKeys(ColumnDefinition colDef, String[] partitionKeys, int partitionKeyCount){
        messageDTO.setKeys(BetterCloudUtil.toString(colDef.name), partitionKeys[partitionKeys.length - 1 - partitionKeyCount]);
        partitionKeyCount++;
    }

    private void assembleClusteringKeys(String[] clusteringKeys, int clusteringKeyCount){
        Iterator<CFDefinition.Name> itClusKeys = cf.metadata().getCfDef().clusteringColumns().iterator();
        if (itClusKeys.hasNext()) {
            CFDefinition.Name clusKeyName = itClusKeys.next();
            messageDTO.setKeys(clusKeyName.name.toString(), clusteringKeys[clusteringKeyCount]);
            clusteringKeyCount++;
        }
    }

    private String getClusteringKeys(ByteBuffer rowKey, Column column){
        AbstractType<?> rowKeyComparator = baseCfs.metadata.getKeyValidator();
        CompositeType baseComparator = (CompositeType) baseCfs.getComparator();
        CFDefinition cfDef = baseCfs.metadata.getCfDef();
        int prefixSize = baseComparator.types.size() - (cfDef.hasCollections ? 2 : 1);
        List<AbstractType<?>> types = baseComparator.types;
        int idx = types.get(types.size() - 1) instanceof ColumnToCollectionType ? types.size() - 2 : types.size() - 1;
        ByteBuffer[] components = baseComparator.split(column.name());
        //String colName = CFDefinition.definitionType.getString(components[idx]);
        StringBuilder sb = new StringBuilder();
        CompositeType.Builder builder = new CompositeType.Builder(baseComparator);
        builder.add(rowKey);
        sb.append(rowKeyComparator.getString(rowKey));
        for (int i = 0; i < Math.min(prefixSize, components.length); i++) {
            builder.add(components[i]);
            AbstractType<?> componentType = types.get(i);
            sb.append(':').append(componentType.compose(components[i]));
        }
        return sb.toString();
    }

    private void assembleRegularColumns(Iterator<Column> itRegCols){
        AbstractType type, keyType, valueType;
        CompositeType baseComparator;
        ByteBuffer[] components;
        CollectionType validator;
        while(itRegCols.hasNext()){
            Column col = itRegCols.next();
            if(baseCfs.metadata.getColumnDefinitionFromColumnName(col.name()) != null){
                type = baseCfs.metadata.getColumnDefinitionFromColumnName(col.name()).getValidator();
                if(!type.isCollection()){
                    messageDTO.setObject(getActualColumnName(col), getString(col.value(), type));
                }else {
                    baseComparator = (CompositeType) baseCfs.getComparator();
                    components = baseComparator.split(col.name());
                    validator = (CollectionType)type;
                    keyType = validator.nameComparator();
                    valueType = validator.valueComparator();
                    if (validator instanceof MapType) {
                        ByteBuffer keyBuf = components[components.length - 1];
                        Map obj = new HashMap<String,String>();
                        obj.put(getActualColumnName(col)+"._key",getString(keyBuf,keyType));
                        obj.put(getActualColumnName(col) + "._value", getString(col.value(), valueType));
                        obj.put(getActualColumnName(col)+"."+keyType.getString(keyBuf).toLowerCase(),getString(col.value(), valueType));
                        messageDTO.setCollectionObject(obj);
                    } else if (validator instanceof SetType) {
                        Map obj = new HashMap<String,String>();
                        obj.put(getActualColumnName(col), getString(components[components.length - 1], keyType));
                        messageDTO.setCollectionObject(obj);
                    } else if (validator instanceof ListType) {
                        Map obj = new HashMap<String,String>();
                        obj.put(getActualColumnName(col), getString(col.value(), valueType));
                        messageDTO.setCollectionObject(obj);
                    }
                }
            }
        }
    }

    public String getActualColumnName(Column column){
        CompositeType baseComparator = (CompositeType) baseCfs.getComparator();
        List<AbstractType<?>> types = baseComparator.types;
        int idx = types.get(types.size() - 1) instanceof ColumnToCollectionType ? types.size() - 2 : types.size() - 1;
        ByteBuffer[] components = baseComparator.split(column.name());
        return CFDefinition.definitionType.getString(components[idx]);
    }

    public String getActualColumnName(ByteBuffer name) {
        ByteBuffer colName = ((CompositeType) baseCfs.getComparator()).extractLastComponent(name);
        return StringUtils.removeStart(CFDefinition.definitionType.getString(colName), ".").trim();
    }

    private String getString(ByteBuffer colValue, AbstractType type){
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
