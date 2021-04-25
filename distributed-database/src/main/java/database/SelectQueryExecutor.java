package database;

import database.DBMSDataTypes.DataType;
import database.Operators.Operator;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;


public class SelectQueryExecutor {

    private static final String DATABASE_ROOT_PATH = "Database";
    private static final Object REMOTE_URL = "https://storage.googleapis.com/csci5408_dbms_remote/Database";
    private String tableName;
    private String databaseName;
    private List<String> lhsConstraint;
    private List<String> rhsConstraint;
    private List<Operator> operatorConstraint;
    private List<String> fieldList;
    private Map<Integer, String> fieldMap;
    private Map<String, String> rowMap;
    private Map<Integer, DataType> tableColumnsDataTypeMap;
    private List<String> conditionalOperators;

    private String location;

    public SelectQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }

    public void setFieldList(List<String> fieldList) {
        this.fieldList = fieldList;
    }

    void populateColumnMap() {
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

    void convertJSONAttributesToDataTypeMap(String dataType) {
        JSONObject columnObject = new JSONObject(dataType);
        JSONArray columnArray = new JSONArray(columnObject.get("columns").toString());
        this.tableColumnsDataTypeMap = new HashMap<>();
        for (int i = 0; i < columnArray.length(); i++) {
            JSONObject columnDetail = new JSONObject(columnArray.get(i).toString());
            DataType dataTypeOfColumn = new DBMSDataTypes().getDataType(columnDetail.getString("columnType"));
            this.tableColumnsDataTypeMap.put(i, dataTypeOfColumn);
        }
    }

    void populateDataTypeMap() {
        try {
            BufferedReader metaReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                URL url = new URL(REMOTE_URL + "/meta");
                metaReader = new BufferedReader(
                        new InputStreamReader(url.openStream()));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/" + "meta.txt";
                metaReader = new BufferedReader(new FileReader(tablePath));
            }
            String rows;
            String dataType = null;
            while ((rows = metaReader.readLine()) != null) {
                String[] row = rows.split("@@@");
                if (row[2].equalsIgnoreCase(this.tableName)) {
                    dataType = row[4];
                    break;
                }
            }
            convertJSONAttributesToDataTypeMap(dataType);
            metaReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    Map<Integer, List<String>> executeSelectStatementWithFullColumns() {
        try {
            Map<Integer, List<String>> selectResult = new HashMap<>();
            BufferedReader tableReader;
            if (location.equalsIgnoreCase("REMOTE")) {
                RemoteFileHandler remoteFileHandler = new RemoteFileHandler(databaseName, tableName);
                remoteFileHandler.downloadObject();
                tableReader = new BufferedReader(new FileReader(DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt" ));
            } else {
                String tablePath = DATABASE_ROOT_PATH + "/" + this.databaseName + '/' + this.tableName + ".txt";
                tableReader = new BufferedReader(new FileReader(tablePath));
            }
            String rows;
            int rowCounter = 0;
            while ((rows = tableReader.readLine()) != null) {
                if (rowCounter > 0) {
                    String[] values = rows.split("\\$");
                    selectResult.put(rowCounter, Arrays.asList(values));
                }
                rowCounter++;
            }
            tableReader.close();
            return selectResult;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private boolean containsIgnoreCase(List<String> columnList, String searchString) {
        for (String column : columnList) {
            if (column.equalsIgnoreCase(searchString)) {
                return true;
            }
        }
        return false;
    }

    List<Integer> filterIndexOfColumn(List<String> fieldList) {
        populateColumnMap();
        List<Integer> filteredIndex = new ArrayList<>();
        for (Map.Entry<Integer, String> column : fieldMap.entrySet()) {
            if (containsIgnoreCase(fieldList, column.getValue())) {
                filteredIndex.add(column.getKey());
            }
        }
        return filteredIndex;
    }


    Map<Integer, List<String>> executeSelectStatementWithColumnList() {
        List<Integer> filteredIndex = filterIndexOfColumn(fieldList);
        Map<Integer, List<String>> filteredSelectResult = new HashMap<>();
        Map<Integer, List<String>> selectResult = executeSelectStatementWithFullColumns();
        return getIntegerListMap(selectResult, filteredIndex, filteredSelectResult);
    }

    Map<Integer, List<String>> executeSelectStatementWithColumnList(Map<Integer, List<String>> selectResult) {
        List<Integer> filteredIndex = filterIndexOfColumn(fieldList);
        Map<Integer, List<String>> filteredSelectResult = new HashMap<>();
        return getIntegerListMap(selectResult, filteredIndex, filteredSelectResult);
    }

    private Map<Integer, List<String>> getIntegerListMap(Map<Integer, List<String>> selectResult, List<Integer> filteredIndex, Map<Integer, List<String>> filteredSelectResult) {
        if (selectResult != null) {
            for (Map.Entry<Integer, List<String>> row : selectResult.entrySet()) {
                List<String> filteredColumnValues = new ArrayList<>();
                for (int i = 0; i < row.getValue().size(); i++) {
                    if (filteredIndex.contains(i)) {
                        filteredColumnValues.add(row.getValue().get(i));
                    }
                }
                filteredSelectResult.put(row.getKey(), filteredColumnValues);
            }
            return filteredSelectResult;
        } else {
            return null;
        }
    }


    private List<DataType> filterDataTypeOfIndex(List<Integer> lhsConstraint) {
        populateDataTypeMap();
        List<DataType> filteredDataTypeOfIndex = new ArrayList<>();
        for (Map.Entry<Integer, DataType> column : tableColumnsDataTypeMap.entrySet()) {
            if (lhsConstraint.contains(column.getKey())) {
                filteredDataTypeOfIndex.add(column.getValue());
            }
        }
        return filteredDataTypeOfIndex;
    }

    Map<Integer, List<String>> executeSelectStatementWithConstraint() {
        List<Integer> filteredIndexOfLhs = filterIndexOfColumn(lhsConstraint);
        List<DataType> loadTypesOfIndex = filterDataTypeOfIndex(filteredIndexOfLhs);
        Map<Integer, List<String>> selectResult = executeSelectStatementWithFullColumns();
        Map<Integer, List<String>> filteredResultOnConstraints = new HashMap<>();
        if (selectResult != null) {
            for (Map.Entry<Integer, List<String>> row : selectResult.entrySet()) {
                if (constraintsMatch(filteredIndexOfLhs, row.getValue(), loadTypesOfIndex)) {
                    filteredResultOnConstraints.put(row.getKey(), row.getValue());
                }
            }
            if (fieldList != null) {
                return executeSelectStatementWithColumnList(filteredResultOnConstraints);
            }
            return filteredResultOnConstraints;
        }
        return null;
    }

    private boolean constraintsMatch(List<Integer> filteredIndexOfLhs, List<String> value, List<DataType> loadTypesOfIndex) {
        List<Boolean> testPassOrder = new ArrayList<>();
        for (int i = 0; i < filteredIndexOfLhs.size(); i++) {
            Operator operator = operatorConstraint.get(i);
            int columnIndex = filteredIndexOfLhs.get(i);
            String actualValue = value.get(columnIndex);
            String expectedValue = rhsConstraint.get(i);
            Operators operators = new Operators(operator);
            if (loadTypesOfIndex.get(i) == DataType.INTEGER) {
                if (operators.performIntComparison(Integer.parseInt(actualValue.trim()), Integer.parseInt(expectedValue))) {
                    testPassOrder.add(true);
                } else {
                    testPassOrder.add(false);
                }
            } else {
                if (operators.performStringComparison(actualValue, expectedValue)) {
                    testPassOrder.add(true);
                } else {
                    testPassOrder.add(false);
                }
            }
        }
        if (conditionalOperators != null) {
            int testPassIndexMover = 0;
            for (String conditionalOperator : conditionalOperators) {
                if (testPassIndexMover + 1 < testPassOrder.size()) {
                    if (conditionalOperator.equalsIgnoreCase("and")) {
                        testPassOrder.set(testPassIndexMover + 1,
                                testPassOrder.get(testPassIndexMover) && testPassOrder.get(testPassIndexMover + 1));
                    }
                    if (conditionalOperator.equalsIgnoreCase("or")) {
                        testPassOrder.set(testPassIndexMover + 1,
                                testPassOrder.get(testPassIndexMover) || testPassOrder.get(testPassIndexMover + 1));
                    }
                    testPassIndexMover++;
                }
            }
            return testPassOrder.get(testPassIndexMover - 1);
        }
        return testPassOrder != null && testPassOrder.get(0);
    }

    void executeSelectMain(String operation, String columns, String conditions) {

        if (conditions != null && !conditions.equalsIgnoreCase("")) {
            Operator operator = null;
            if (conditions.contains("=")) {
                operator = Operator.EQUAL_TO;
            }
            if (conditions.contains(">")) {
                operator = Operator.GREATER_THAN;
            }
            if (conditions.contains("<")) {
                operator = Operator.LESSER_THAN;
            }
            if (conditions.contains("!=")) {
                operator = Operator.NOT_EQUAL;
            }
            if (conditions.contains(">=")) {
                operator = Operator.GREATER_THAN_EQUALS;
            }
            if (conditions.contains("<=")) {
                operator = Operator.LESSER_THAN_EQUALS;
            }
            operatorConstraint = new ArrayList<>();
            operatorConstraint.add(operator);
            String[] operands = conditions.split(operator.getOperatorString());
            lhsConstraint = new ArrayList<>();
            lhsConstraint.add(operands[0]);
            rhsConstraint = new ArrayList<>();
            rhsConstraint.add(operands[1]);
            if (!columns.equalsIgnoreCase("*")) {
                fieldList = Arrays.asList(columns.split(","));
            }
            System.out.println(executeSelectStatementWithConstraint().values());
        } else {
            if (columns.equalsIgnoreCase("*")) {
                System.out.println("Successfully fetched records");
                populateColumnMap();
                System.out.println(fieldMap);
                System.out.println(executeSelectStatementWithFullColumns().values());
            } else {
                fieldList = Arrays.asList(columns.split(","));
                System.out.println("Successfully fetched records");
                System.out.println(fieldList);
                System.out.println(executeSelectStatementWithColumnList().values());
            }
        }
    }

    public static void main(String args[]) {
        SelectQueryExecutor sqe = new SelectQueryExecutor("students", "test", "local");
        String operation = "Select";
        String columns = "*";
        String conditions = "English<90";
        sqe.executeSelectMain(operation, columns, conditions);
    }

}
