/*
 * Copyright 2014, Stratio.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bettercloud.cassandra;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CBuilder;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.filter.ColumnSlice;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class ClusteringKeyMapper
{
    protected final CFMetaData metadata;

    protected final CellNameType cellNameType;

    protected final CompositeType compositeType;

    private final int numClusteringColumns;

    protected static final Logger logger = LoggerFactory.getLogger(ClusteringKeyMapper.class);

    public ClusteringKeyMapper(CFMetaData metadata)
    {
        this.metadata = metadata;
        this.cellNameType = metadata.comparator;
        this.compositeType = (CompositeType) cellNameType.asAbstractType();
        numClusteringColumns = metadata.clusteringColumns().size();
    }

    public final CellNameType getType()
    {
        return cellNameType;
    }

    public final CellName clusteringKey(ColumnFamily columnFamily)
    {
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (isClusteringKey(cellName))
            {
                return cellName;
            }
        }
        return null;
    }

    public final String[] clusteringKeys(ColumnFamily columnFamily)
    {
        String[] clusteringKeys = null;
        CellName lastClusteringKey = null;
        if(columnFamily.iterator().hasNext()){
            for (Cell cell : columnFamily)
            {
                CellName cellName = cell.name();
                if (!isStatic(cellName))
                {
                    clusteringKeys = extractClusteringKey(cellName);
                }
            }
        }else if (columnFamily.deletionInfo() != null){
            DeletionInfo deletionInfo = columnFamily.deletionInfo();
            Iterator<RangeTombstone> iterator = deletionInfo.rangeIterator();
            while (iterator.hasNext())
            {
                RangeTombstone rangeTombstone = iterator.next();
                clusteringKeys = query(rangeTombstone.min,rangeTombstone.max);
            }
        }
        return clusteringKeys;
    }

    protected final String[] extractClusteringKey(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
        String[] clusKeys = new String[numClusteringColumns];
        for (int i = 0; i < numClusteringColumns; i++)
        {
            clusKeys[i] = BetterCloudUtil.getString(cellName.get(i), cellNameType.subtype(i));
        }
        return clusKeys;
    }

    protected final boolean isStatic(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
        for (int i = 0; i < numClusteringColumns; i++)
        {
            if (BetterCloudUtil.isEmpty(cellName.get(i))) // Ignore static columns
            {
                return true;
            }
        }
        return false;
    }

    protected final boolean isClusteringKey(CellName cellName)
    {
        int numClusteringColumns = metadata.clusteringColumns().size();
        for (int i = 0; i < numClusteringColumns; i++)
        {
            if (BetterCloudUtil.isEmpty(cellName.get(i))) // Ignore static columns
            {
                return false;
            }
        }
        return BetterCloudUtil.isEmpty(cellName.get(numClusteringColumns));
    }

    public final Composite start(CellName cellName)
    {
        CBuilder builder = cellNameType.builder();
        for (int i = 0; i < cellName.clusteringSize(); i++)
        {
            ByteBuffer component = cellName.get(i);
            builder.add(component);
        }
        return builder.build();
    }

    public final Composite end(CellName cellName)
    {
        return start(cellName).withEOC(Composite.EOC.END);
    }

    public final Map<CellName, ColumnFamily> splitRows(ColumnFamily columnFamily)
    {
        Map<CellName, ColumnFamily> columnFamilies = new LinkedHashMap<>();
        ColumnFamily rowColumnFamily = null;
        CellName clusteringKey = null;
        for (Cell cell : columnFamily)
        {
            CellName cellName = cell.name();
            if (isClusteringKey(cellName))
            {
                if (rowColumnFamily != null)
                {
                    columnFamilies.put(clusteringKey, rowColumnFamily);
                }
                clusteringKey = cellName;
                rowColumnFamily = ArrayBackedSortedColumns.factory.create(metadata);
            }
            rowColumnFamily.addColumn(cell);
        }
        if (rowColumnFamily != null)
        {
            columnFamilies.put(clusteringKey, rowColumnFamily);
        }
        return columnFamilies;
    }

    public final ColumnSlice[] columnSlices(List<CellName> clusteringKeys)
    {
        List<CellName> sortedClusteringKeys = sort(clusteringKeys);
        ColumnSlice[] columnSlices = new ColumnSlice[clusteringKeys.size()];
        int i = 0;
        for (CellName clusteringKey : sortedClusteringKeys)
        {
            Composite start = start(clusteringKey);
            Composite end = end(clusteringKey);
            ColumnSlice columnSlice = new ColumnSlice(start, end);
            columnSlices[i++] = columnSlice;
        }
        return columnSlices;
    }

    public final List<CellName> sort(List<CellName> clusteringKeys)
    {
        List<CellName> result = new ArrayList<>(clusteringKeys);
        Collections.sort(result, new Comparator<CellName>()
        {
            @Override
            public int compare(CellName o1, CellName o2)
            {
                return cellNameType.compare(o1, o2);
            }
        });
        return result;
    }

       public final String toString(Composite cellName)
    {
        return BetterCloudUtil.toString(cellName.toByteBuffer());
    }

    public String[] query(Composite start, Composite stop)
    {
        List<ColumnDefinition> clusteringColumns = metadata.clusteringColumns();
        String[] clusKeys = new String[numClusteringColumns];
        if (start != null && !start.isEmpty())
        {
            for (int i = 0; i < numClusteringColumns; i++)
            {
                ColumnDefinition columnDefinition = clusteringColumns.get(i);
                String name = columnDefinition.name.toString();
                AbstractType type = columnDefinition.type;
                ByteBuffer value = start.get(i);
                clusKeys[i] = BetterCloudUtil.getString(value,type);
            }
        }

        if (stop != null && !stop.isEmpty())
        {
            for (int i = 0; i < numClusteringColumns; i++)
            {
                ColumnDefinition columnDefinition = clusteringColumns.get(i);
                String name = columnDefinition.name.toString();
                AbstractType type = columnDefinition.type;
                ByteBuffer value = stop.get(i);
                clusKeys[i] = BetterCloudUtil.getString(value,type);
            }
        }
        return clusKeys;
    }
}
