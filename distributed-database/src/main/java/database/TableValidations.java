package database;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public class TableValidations {

    private static final String DATABASE_ROOT_PATH = "Database";
    private static final Object REMOTE_URL = "https://storage.googleapis.com/csci5408_dbms_remote/Database";
    private String tableName;
    private String databaseName;
    private String[] columns;
    private String[] values;
    private Map<Integer, String> fieldMap;
    private Map<String, String> inputFieldMap = new HashMap<>();
    private String location;

    public TableValidations(String tableName, String databaseName, String[] columns, String[] values, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.columns = columns;
        this.values = values;
        this.location = location;
        if(!(columns == null || values == null)) {
            for (int i = 0; i < columns.length; i++) {
                inputFieldMap.put(columns[i], values[i]);
            }
        }
    }

    public String[] getColumns() {
        try {BufferedReader metaReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                URL url = new URL(REMOTE_URL + "/meta");
                metaReader = new BufferedReader(
                        new InputStreamReader(url.openStream()));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/meta.txt";
                metaReader = new BufferedReader(new FileReader(tablePath));
            }
            String rows;
            while ((rows = metaReader.readLine()) != null) {
                String[] row = rows.split("@@@");
                if (row[2].equalsIgnoreCase(this.tableName)) {
                    return row;
                }
            }
            metaReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void populateColumnMap() {
        try {
            BufferedReader tableReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                RemoteFileHandler remoteFileHandler = new RemoteFileHandler(databaseName, tableName);
                remoteFileHandler.downloadObject();
                tableReader = new BufferedReader(new FileReader(DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt" ));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt";
                tableReader = new BufferedReader(new FileReader(tablePath));
            }
            String rows = tableReader.readLine();
            String[] columns = rows.split("\\$");

            fieldMap = new HashMap<>();
            for (int i = 0; i < columns.length; i++) {
                fieldMap.put(i, columns[i]);
            }
            tableReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    int getIndexOfKeyName(String keyName) {
        populateColumnMap();
        for (Map.Entry<Integer, String> columns : fieldMap.entrySet()) {
            if (columns.getValue().toString().equals(keyName)) {
                return (int) columns.getKey();
            }
        }
        return -1;
    }

    public boolean checkPrimaryKey(String actualTableName) {
        // selecting all records of Schema table
        SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(tableName, databaseName, location);
        Map<Integer, List<String>> resultSet = selectQueryExecutor.executeSelectStatementWithFullColumns();

        // position of primary_key column in schema file
        int indexOfKey = getIndexOfKeyName("primary_key");
        String columnName = getPrimaryKey();
        // looping the schema table records
        for (Map.Entry<Integer, List<String>> rows : resultSet.entrySet()) {
            try {
                List<String> rowDetails = rows.getValue();
                if (columnName != null) {
                    SelectQueryExecutor selectQueryExecutorActual = new SelectQueryExecutor(actualTableName, databaseName, location);
                    List<String> list = new ArrayList<>();
                    list.add(columnName);
                    selectQueryExecutorActual.setFieldList(list);
                    Map<Integer, List<String>> resultSetActual = selectQueryExecutorActual.executeSelectStatementWithColumnList();
                    for (List<String> i : resultSetActual.values()) {
                        if (i.get(0).equals(inputFieldMap.get(columnName))) {
                            System.out.println("Primary Key Violated!");
                            return false;
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
        }
        return true;
    }

    private String getPrimaryKey() {
        if (getColumns() != null) {
            return getColumns()[3];
        }
        return null;
    }

    private String getForeignKey() {
        if (getColumns() != null) {
            return getColumns()[5];
        }
        return null;
    }

    private String getColumnsArray() {
        if (getColumns() != null) {
            return getColumns()[4];
        }
        return null;
    }
    public boolean checkForeignKey(String actualTable) {
        String foreignKey = getForeignKey();
        if (foreignKey == null) {
            return true;
        }
        if (foreignKey != null) {
            JSONObject foreignKeyObj = new JSONObject(foreignKey);
            JSONArray foreignKeyArrays = new JSONArray(foreignKeyObj.get("keys").toString());
            if(foreignKeyArrays.length() == 0) {
            	return true;
            }
            for (int fk = 0; fk < foreignKeyArrays.length(); fk++) {
                JSONObject foreignKeyConstraintObj = new JSONObject(foreignKeyArrays.get(fk).toString());
                // department_id
                String foreignKeyColumnInTable = foreignKeyConstraintObj.getString("column");
                // id
                String foreignKeyColumnName = foreignKeyConstraintObj.getString("foreignKeyColumn");
                // department
                String foreignKeyTableName = foreignKeyConstraintObj.getString("foreignKeyTable");
                // query executor to query department table
                SelectQueryExecutor selectQueryExecutor = new SelectQueryExecutor(foreignKeyTableName, databaseName, location);
                List<String> list = new ArrayList<>();
                list.add(foreignKeyColumnName);
                selectQueryExecutor.setFieldList(list);
                Map<Integer, List<String>> foreignTableResult = selectQueryExecutor.executeSelectStatementWithColumnList();
                for (List<String> i : foreignTableResult.values()) {
                    if(i.get(0).equals(inputFieldMap.get(foreignKeyColumnInTable))){
                        System.out.println("Good to insert");
                        return true;
                    }
                }
            }
        }
        System.out.println("Foreign key violation");
        return false;
    }

    public boolean checkDataTypes(String metaTableName) {
        String columnsArray = getColumnsArray();
        boolean result = false;
        if (columnsArray != null) {
            JSONObject columnsArrayObject = new JSONObject(columnsArray);
            JSONArray columnsArrays = new JSONArray(columnsArrayObject.get("columns").toString());
            Map<String, String> dataTypeMap = new HashMap<>();
            for (int c = 0; c < columnsArrays.length(); c++) {
                try {
                JSONObject columnArrayObj = new JSONObject(columnsArrays.get(c).toString());
                String columnType = columnArrayObj.getString("columnType");
                String columnName = columnArrayObj.getString("columnName");
                dataTypeMap.put(columnName, columnType);
                } catch (ArrayIndexOutOfBoundsException e) {
                }
            }
            for (int i = 0; i < columns.length; i++) {
                String dataType = (String) dataTypeMap.get(columns[i]).trim();
                result = checkDataTypeWithValues(dataType, values[i], columns[i]);
                if (result == false) {
                    break;
                }
            }
        }
        return result;
    }

    public boolean checkDataTypeWithValues(String dataType, String value, String column) {
        String stripDataType = dataType.replaceAll("\\s+", "");
        String stripValue = value.replaceAll("\\s+", "");
        if (dataType.equals("varchar")) {
            if (stripValue.length() == 0) {
                System.out.println("Empty String");
                return false;
            }
        } else if (stripDataType.equals("int")) {
            try {
                Integer i = Integer.parseInt(stripValue);
            }
            catch(NumberFormatException e){
                System.out.println("Datatype mismatch. " + column + " must be int");
                return false;
            }
        } else if (stripDataType.equals("float")) {
            float f = Float.parseFloat(value);
        } else if (stripDataType.equals("bool")) {
            if (stripValue.equals("true") || stripValue.equals("false")) {

            }
            else {
                System.out.println("Not a boolean!");
                return false;
            }
        }
        return true;
    }

    public boolean validateColumnCount(String[] columns, String[] values) {
        if (columns.length != values.length) {
            System.out.println("Column count and Values count does not match!");
            return false;
        }
        return true;
    }

    public boolean checkIfTableNameValid(String tableName) {
        List<String> tableNames = getTableNames();
        for(String table : tableNames) {
            if (table.equalsIgnoreCase(tableName)) {
                System.out.println("Table already exists");
                return false;
            }
        }
        return true;
    }

    private List<String> getTableNames() {
        List<String> tableList = new ArrayList<>();
        try {
            BufferedReader metaReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                URL url = new URL(REMOTE_URL + "/meta");
                metaReader = new BufferedReader(
                        new InputStreamReader(url.openStream()));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt";
                metaReader = new BufferedReader(new FileReader(tablePath));
            }
            String rows;
            while ((rows = metaReader.readLine()) != null) {
                String[] row = rows.split("@@@");
                tableList.add(row[2]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tableList;
    }
}

