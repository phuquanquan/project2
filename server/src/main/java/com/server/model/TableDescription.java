package main.java.com.server.model;

import java.util.List;

public class TableDescription {

    private final String tableName;
    private final List<String> columnFamilies;

    public TableDescription() {
        this.tableName = null;
        this.columnFamilies = null;
    }

    public TableDescription(String tableName) {
        this.tableName = tableName;
        this.columnFamilies = null;

    }

    public TableDescription(String tableName, List<String> columnFamilies) {
        this.tableName = tableName;
        this.columnFamilies = columnFamilies;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumnFamilies() {
        return columnFamilies;
    }

    @Override
    public String toString() {
        return "TableDescription{" +
                "tableName='" + tableName + '\'' +
                ", columnFamilies=" + columnFamilies +
                '}';
    }
}
