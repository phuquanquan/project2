package main.java.com.server.model;

import java.util.List;

public class ColumnFamilyValue {
    private final String familyName;
    private final List<ColumnValue> columnValues;

    public ColumnFamilyValue() {
        this.familyName = null;
        this.columnValues = null;
    }

    public ColumnFamilyValue(String familyName, List<ColumnValue> columnValues) {
        this.familyName = familyName;
        this.columnValues = columnValues;
    }

    public String getFamilyName() {
        return familyName;
    }

    public List<ColumnValue> getColumnValues() {
        return columnValues;
    }

    @Override
    public String toString() {
        return "ColumnFamilyValue{" +
                "familyName='" + familyName + '\'' +
                ", columnValues=" + columnValues +
                '}';
    }
}
