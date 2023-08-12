package main.java.com.server.service;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.springframework.stereotype.Service;
import main.java.com.server.model.ColumnFamilyValue;
import main.java.com.server.model.ColumnValue;
import main.java.com.server.model.RowValue;
import main.java.com.server.model.TableDescription;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;

@Service
public class ConversionsService {

    public TableDescription constructTableDescription(HTableDescriptor htd) {
        TableDescription result = null;

        if (htd != null) {
            String name = htd.getNameAsString();
            Collection<HColumnDescriptor> families = htd.getFamilies();

            List<String> familiesNames = families.stream()
                    .map(HColumnDescriptor::getNameAsString)
                    .collect(Collectors.toList());

            result = new TableDescription(name, familiesNames);
        }

        return result;
    }

    public RowValue constructRowValue(Result r) {
        RowValue result = null;

        if (r != null && !r.isEmpty()) {
            String rowKey = new String(r.getRow());

            // Map<family,Map<qualifier,value>>
            NavigableMap<byte[], NavigableMap<byte[], byte[]>> valueMap = r.getNoVersionMap();

            List<ColumnFamilyValue> families = valueMap.entrySet().stream()
                    .map(b -> new ColumnFamilyValue(new String(b.getKey()), constructColumnValues(b.getValue())))
                    .collect(Collectors.toList());

            result = new RowValue(rowKey, families);
        }

        return result;
    }

    private List<ColumnValue> constructColumnValues(NavigableMap<byte[], byte[]> map) {
        List<ColumnValue> result = null;
        result = map.entrySet().stream()
                .map(i -> new ColumnValue(new String(i.getKey()), new String(i.getValue())))
                .collect(Collectors.toList());
        return result;
    }

}
