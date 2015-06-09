package com.bettercloud.cassandra;

import org.apache.cassandra.config.ColumnDefinition;
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

    public CassandraRowAssembler(ColumnFamilyStore baseCfs, ByteBuffer rowKey, ColumnFamily cf){
        this.baseCfs = baseCfs;
        this.rowKey = rowKey;
        this.cf = cf;
        messageDTO = new MessageDTO();
        messageDTO.setTimestamp(cf.maxTimestamp()+"");
        this.assemble();
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
        Iterator<Cell> itRegCols = cf.iterator();

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
        if(cf.isMarkedForDelete() && !cf.iterator().hasNext()){
            messageDTO.setOperation(Operation.delete.name());
        }else{
            messageDTO.setOperation(Operation.insert.name());
        }
    }

    private void assemblePartitionKeys(ColumnDefinition colDef, String[] partitionKeys, int partitionKeyCount){
        messageDTO.setKeys(colDef.name.toString(), partitionKeys[partitionKeys.length - 1 - partitionKeyCount]);
        partitionKeyCount++;
    }

    private void assembleClusteringKeys(){
        ClusteringKeyMapper clusteringKeyMapper = new ClusteringKeyMapper(baseCfs.metadata);
        String keyname;
        String[] listClusterKey  = clusteringKeyMapper.clusteringKeys(cf);
        Iterator<ColumnDefinition> clusCols = baseCfs.metadata.clusteringColumns().iterator();
        for(int i = 0; listClusterKey!= null && i< listClusterKey.length ; i++){
            keyname = clusCols.next().name.toString();
            messageDTO.setKeys(keyname, listClusterKey[i]);
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
                messageDTO.setObject(name, BetterCloudUtil.getString(cell.value(), valueType));
            }else {
                collectionType = (CollectionType<?>) valueType;
                switch(collectionType.kind){
                    case SET:{
                        Map obj = new HashMap<String,String>();
                        obj.put(name, BetterCloudUtil.getString(cellName.collectionElement(), collectionType.nameComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                    case LIST:{
                        Map obj = new HashMap<String,String>();
                        obj.put(name, BetterCloudUtil.getString(cellValue, collectionType.valueComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                    case MAP:{
                        Map obj = new HashMap<String,String>();
                        obj.put(name + "._key",BetterCloudUtil.getString(cellName.collectionElement(), collectionType.nameComparator()));
                        obj.put(name + "._value", BetterCloudUtil.getString(cellValue, collectionType.valueComparator()));
                        messageDTO.setCollectionObject(obj);
                        break;
                    }
                }
            }
        }
    }
}
