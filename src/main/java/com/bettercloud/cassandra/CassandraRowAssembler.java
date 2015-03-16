package com.bettercloud.cassandra;

import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.db.Cell;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
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
        int partitionKeyCount = 0;
        String partitionKeyStringPart = "";
        String[] partitionKeys;
        partitionKeyStringPart = baseCfs.metadata.getKeyValidator().getString(rowKey);
        partitionKeys = partitionKeyStringPart.split(":");
        Iterator<ColumnDefinition> itColDef = cf.metadata().allColumns().iterator();
        String keys = "";
        Iterator<Cell> itRegCols = cf.iterator();

        logger.warn("Decorated Key : " + baseCfs.partitioner.decorateKey(rowKey));
        if(itRegCols.hasNext()){
            logger.warn("In Loop");
            logger.warn(BetterCloudUtil.toString(itRegCols.next().value()));

        }
        itRegCols = cf.iterator();

        while(itColDef.hasNext()){
            ColumnDefinition colDef = itColDef.next();

            switch (colDef.kind) {
                case PARTITION_KEY: {
                    if(partitionKeys.length > 0){
                        assemblePartitionKeys(colDef, partitionKeys, partitionKeyCount);
                    }
                }
                break;
                case CLUSTERING_COLUMN: {
                    assembleClusteringKeys();
                }
                break;
                case REGULAR: {
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
        messageDTO.setKeys(colDef.name.toString(), partitionKeys[partitionKeys.length - 1 - partitionKeyCount]);
        partitionKeyCount++;
    }

    private void assembleClusteringKeys(){
        ClusteringKeyMapper clusteringKeyMapper = new ClusteringKeyMapper(baseCfs.metadata);
        String[] listClusterKey  = clusteringKeyMapper.clusteringKeys(cf);
        Iterator<ColumnDefinition> clusCols = baseCfs.metadata.clusteringColumns().iterator();
        for(int i = 0; listClusterKey!= null && i< listClusterKey.length ; i++){
            messageDTO.setKeys(clusCols.next().name.toString(), listClusterKey[i]);
        }
    }

    private void assembleRegularColumns(Iterator<Cell> itRegCols){
        AbstractType type, keyType, valueType;
        CollectionType collectionType;
        ByteBuffer[] components;
        CollectionType validator;
        while(itRegCols.hasNext()){
            Cell cell = itRegCols.next();
            CellName cellName = cell.name();
            String name = cellName.cql3ColumnName(baseCfs.metadata).toString();
            ColumnDefinition columnDefinition = baseCfs.metadata.getColumnDefinition(cellName);
            if (columnDefinition == null)
            {
                continue;
            }
            ByteBuffer cellValue = cell.value();
            valueType = columnDefinition.type;
            if(!valueType.isCollection()){
                messageDTO.setObject(name, getString(cell.value(), valueType));
            }else {
                collectionType = (CollectionType<?>) valueType;
                switch(collectionType.kind){
                    case SET:{
                        Map obj = new HashMap<String,String>();
                        obj.put(name, getString(cellName.collectionElement(), collectionType.nameComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                    case LIST:{
                        Map obj = new HashMap<String,String>();
                        obj.put(name, getString(cellValue, collectionType.valueComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                    case MAP:{
                        logger.warn("In Map");
                        Map obj = new HashMap<String,String>();
                        obj.put(name + "._key",getString(cellName.collectionElement(),collectionType.nameComparator()));
                        obj.put(name + "._value", getString(cellValue, collectionType.valueComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                }
            }
        }
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
