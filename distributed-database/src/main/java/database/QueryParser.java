package database;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryParser {

    private static String databaseName;

    public QueryParser() {
    }

    public boolean parsingQuery(String query, boolean isTransaction) {

        if (query != null && query.trim().length() > 0) {

            query = query.trim();

            query.replaceAll("\\s+", " ");

            Pattern pattern = Pattern.compile(
                    "^(SELECT |UPDATE |INSERT |DELETE |CREATE TABLE |CREATE DATABASE |USE |BEGIN TRANSACTION |DROP TABLE |TRUNCATE TABLE |SHOW DATABASES|SHOW TABLES|DESC |DESCRIBE )",
                    Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(query);
            boolean matchFound = matcher.find();
            if (matchFound) {
                String queryType = matcher.group();
                System.out.println("Match found");
                if (queryType.trim().equalsIgnoreCase("CREATE DATABASE") && !isTransaction) {
                    tokenizeCreateDatabaseQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("USE") && !isTransaction) {
                    tokenizeUseDatabaseQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("SHOW DATABASES") && !isTransaction) {
                    tokenizeShowDatabaseQuery(pattern, matcher, query);
                } else if (StringUtils.isBlank(QueryParser.databaseName)) {
                    System.out.println("Please specify database");
                } else if (queryType.trim().equalsIgnoreCase("SHOW TABLES") && !isTransaction) {
                    tokenizeShowTablesQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("SELECT") && !isTransaction) {
                    tokenizeSelectQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("UPDATE")) {
                    tokenizeUpdateQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("INSERT")) {
                    tokenizeInsertQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("DELETE")) {
                    tokenizeDeleteQuery(pattern, matcher, query, isTransaction);
                } else if (queryType.trim().equalsIgnoreCase("CREATE TABLE") && !isTransaction) {
                    tokenizeCreateTableQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("BEGIN TRANSACTION") && !isTransaction) {
                    tokenizeTransactionQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("DROP TABLE") && !isTransaction) {
                    tokenizeDropQuery(pattern, matcher, query);
                } else if (queryType.trim().equalsIgnoreCase("TRUNCATE TABLE") && !isTransaction) {
                    tokenizeTruncateQuery(pattern, matcher, query);
                } else if ((queryType.trim().equalsIgnoreCase("DESC")
                        || queryType.trim().equalsIgnoreCase("DESCRIBE"))
                        && !isTransaction) {
                    tokenizeDescribeQuery(pattern, matcher, query);
                }
            } else {
                System.out.println("Query syntax is not correct, please check keywords spellings and order.");
                return false;
            }
        }

        return true;
    }

    private void tokenizeShowTablesQuery(Pattern pattern, Matcher matcher, String query) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("Database/meta.txt"));
            System.out.println("Databases present are:");
            System.out.println("Table name ___ Location");
            String line;
            boolean isDatabase = true;
            while ((line = reader.readLine()) != null) {
                String[] components = line.split("@@@");
                if (components[1].equalsIgnoreCase(databaseName)) {
                    isDatabase = true;
                    System.out.println(components[2] + "___" + components[0]);
                }
            }
            if (!isDatabase) {
                System.out.println("no such database exists");
            }
            reader.close();
        } catch (Exception e) {
            System.out.println("Query Failed!");

        }
    }

    private void tokenizeDescribeQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile(
                "(DESCRIBE |DESC)\\s+([\\w]+)\\s*$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);
        if (matcher.find()) {
            String tableName = matcher.group(2);
            System.out.println(tableName);
            try {
                BufferedReader reader = new BufferedReader(new FileReader("Database/meta.txt"));
                String line;
                boolean isTableFound = false;
                while ((line = reader.readLine()) != null) {
                    String[] components = line.split("@@@");
                    if (components[2].equalsIgnoreCase(tableName)
                            && components[1].equalsIgnoreCase(databaseName)) {
                        isTableFound = true;
                        System.out.println("Column Name ___ Column Type ___ PK");
                        String pk = components[3];
                        JSONObject columnsObj = new JSONObject(components[4]);
                        JSONArray columnArray = new JSONArray(columnsObj.get("columns").toString());
                        for (int i = 0; i < columnArray.length(); i++) {
                            JSONObject column = new JSONObject(columnArray.get(i).toString());
                            if (pk.equalsIgnoreCase(column.getString("columnName"))) {
                                System.out.println(column.getString("columnName")
                                        + "___" + column.getString("columnType")
                                        + "___YES");
                            } else {
                                System.out.println(column.getString("columnName")
                                        + "___" + column.getString("columnType")
                                        + "___NO");
                            }
                        }
                    }
                }
                if (!isTableFound) {
                    System.out.println("Table Not Found! Check table name");
                }

            } catch (Exception e) {
                System.out.println("Error in your SQL Syntax. Please check");
            }
        }

    }

    private void tokenizeShowDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("Database/meta.txt"));
            System.out.println("Databases present are:");
            System.out.println("Database name ___ Location");
            String line;
            Map<String, Boolean> databaseMap = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                String[] components = line.split("@@@");
                if (!databaseMap.containsKey(components[1] + " ___" + components[0])) {
                    databaseMap.put(components[1] + " ___" + components[0], true);
                }
            }
            for (Map.Entry<String, Boolean> entry : databaseMap.entrySet()) {
                System.out.println(entry.getKey());
            }
            reader.close();

        } catch (Exception e) {

        }
    }

    private void writeDump(String query) {
        try {
            System.out.println(databaseName);
            File file = new File("Database/" + databaseName + "_dump.txt");
            FileWriter fstream = new FileWriter(file, true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(query);
            out.newLine();
            out.flush();
            out.close();
        } catch (Exception e) {
            System.out.println("Failed writing dump");
        }
    }

    private void tokenizeTransactionQuery(Pattern pattern, Matcher matcher, String query) {
        try {
            TransactionManager transactionManager = new TransactionManager(databaseName);
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            String line = "";
            while (!line.equalsIgnoreCase("END")) {
                line = in.readLine();
                if (parsingQuery(line, true)) {
                    transactionManager.addTransaction(line);
                }
            }
            in.close();
            transactionManager.endAndExecuteTransaction();
        } catch (Exception e) {
            System.out.println("Transaction failed");
            e.printStackTrace();
        }
    }

    private void tokenizeInsertQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile(
                "(INSERT)\\s+INTO\\s+([\\w]+)\\s*(\\(([\\w, ]+)\\))?\\s*\\s+VALUES\\s+\\(([\\w, ]+)\\)$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String columnName = matcher.group(4);
            String[] columnNames = null;
            if (columnName != null) {
                columnNames = columnName.split(",");
            }
            String[] columnValues = matcher.group(5).split(",");

            try {
                String location = getLocation(tableName);
                TableValidations tableValidations = new TableValidations(tableName, databaseName, columnNames,
                        columnValues, location);
                if (tableValidations.checkPrimaryKey(tableName) && tableValidations.checkDataTypes("meta")  && !isTransaction) {
                    InsertQueryExecutor insertQueryExecutor = new InsertQueryExecutor(tableName, databaseName, location);
                    insertQueryExecutor.performInsertQueryOperation(columnValues);
                } else {
                    System.out.println("Constraint error(s) found!");
                }
            } catch (IOException e) {
                System.out.println("Cannot write to database" + e.getMessage());
                e.getStackTrace();
            }
            writeDump(query);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeDeleteQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile("(DELETE)\\s+FROM\\s+([\\w]+)\\s* WHERE\\s+(([\\S]+))?$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String conditions = matcher.group(3);

            if (!isTransaction) {
                String location = getLocation(tableName);
                DeleteQueryExecutor deleteQueryExecutor = new DeleteQueryExecutor(tableName, databaseName, location);
                deleteQueryExecutor.splitCondition(conditions);
            }
            writeDump(query);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeSelectQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(SELECT)\\s+(\\*|[\\w, ]+)\\s+FROM\\s+([\\w]+)\\s*( WHERE\\s+([\\S]+))?$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String columns = matcher.group(2);
            String tableName = matcher.group(3);
            String conditions = matcher.group(5);
            String location = getLocation(tableName);
            if (location != null) {
                SelectQueryExecutor sqe = new SelectQueryExecutor(tableName, databaseName, location);
                sqe.executeSelectMain(operation, columns, conditions);
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeUpdateQuery(Pattern pattern, Matcher matcher, String query, boolean isTransaction) {
        pattern = Pattern.compile("(UPDATE)\\s+([\\w]+)\\s+SET\\s+([\\S]+)\\s*( WHERE\\s+([\\S]+))?$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String tableName = matcher.group(2);
            String updateOperations = matcher.group(3);
            String conditions = matcher.group(5);
            if (!isTransaction) {
                String location = getLocation(tableName);
                if (location != null) {
                    UpdateQueryExecutor uqe = new UpdateQueryExecutor(tableName, databaseName, location);
                    uqe.executeUpdateMain(updateOperations, conditions);
                }
            }
            writeDump(query);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeCreateTableQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(CREATE)\\s+(TABLE)\\s+([\\w]+)\\s*\\(([\\w, \\(\\)]+)\\)$",
                Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String operation = matcher.group(1);
            String subOperation = matcher.group(2);
            String tableName = matcher.group(3);
            String columnsDesc = matcher.group(4);

            String[] columnDescArray = columnsDesc.split(",");

            List<String> columns = new ArrayList<String>();
            JSONObject tableColumnsObject = new JSONObject();
            JSONObject tableForeignKeysObject = new JSONObject();
            String primaryKey = null;

            JSONArray foreignKeyArray = new JSONArray();
            JSONArray columnArray = new JSONArray();
            JSONObject foreignKeyObj = null;
            JSONObject columnObj = null;

            for (String columnDesc : columnDescArray) {
                pattern = Pattern.compile(
                        "([\\w]+)\\s*( INT| VARCHAR)\\s*( PRIMARY KEY| REFERENCES\\s+([\\w]+)\\((\\w+)\\))?\\s*$",
                        Pattern.CASE_INSENSITIVE);
                matcher = pattern.matcher(columnDesc);
                if (matcher.find()) {
                    columnObj = new JSONObject();
                    String columnName = matcher.group(1);
                    String columnType = matcher.group(2);
                    String constraint = matcher.group(3);
                    String foreignKeyTable = matcher.group(4);
                    String foreignKeyColumn = matcher.group(5);

                    columnObj.put("columnName", columnName.trim());
                    columns.add(columnName.trim());
                    columnObj.put("columnType", columnType.trim());
                    columnArray.put(columnObj);

                    if (constraint != null && constraint.trim().length() > 0
                            && constraint.trim().equalsIgnoreCase("PRIMARY KEY")) {
                        primaryKey = columnName.trim();
                    }

                    if (foreignKeyTable != null && foreignKeyTable.trim().length() > 0) {
                        foreignKeyObj = new JSONObject();
                        foreignKeyObj.put("column", columnName);
                        foreignKeyObj.put("foreignKeyTable", foreignKeyTable.trim());
                        foreignKeyObj.put("foreignKeyColumn", foreignKeyColumn.trim());
                        foreignKeyArray.put(foreignKeyObj);
                    }

                } else {
                    System.out.println("Please check syntax of the command.");
                }
            }
            tableColumnsObject.put("columns", columnArray);
            tableForeignKeysObject.put("keys", foreignKeyArray);
            String location = getLocation(tableName);
            TableValidations taskValidation = new TableValidations(tableName, databaseName, columnDescArray, null, location);
            if (location == null) {
                CreateTableQuery createTableQuery = new CreateTableQuery();
                createTableQuery.exceuteCreateTableQuery(QueryParser.databaseName, tableName, primaryKey, columns,
                        tableColumnsObject, tableForeignKeysObject);
                writeDump(query);
            } else {
                System.out.println("table already exists");
            }

        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeCreateDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(CREATE)\\s+(DATABASE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String databaseName = matcher.group(3);
            CreateDatabaseQuery createDatabaseQuery = new CreateDatabaseQuery();
            createDatabaseQuery.exceuteCreateDatabaseQuery(databaseName);
            writeDump(query);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeUseDatabaseQuery(Pattern pattern, Matcher matcher, String query) {
//        pattern = Pattern.compile("(USE)\\s+(DATABASE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
    	pattern = Pattern.compile("(USE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String databaseName = matcher.group(2);
            QueryParser.databaseName = databaseName;
            writeDump(query);
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private void tokenizeDropQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(DROP)\\s+(TABLE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String tableName = matcher.group(3);
            try {
                String location = getLocation(tableName);
                DropQueryExecutor dropQueryExecutor = new DropQueryExecutor(tableName, databaseName, location);
                dropQueryExecutor.performDropQueryOperation();
                writeDump(query);
            } catch (IOException e) {
                System.out.println("Cannot DROP table" + e.getMessage());
                e.getStackTrace();
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    private String getLocation(String tableName) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader("Database" + "/" + "meta.txt"));
            String line = reader.readLine();
            while (line != null) {
                String[] arr = line.split("@@@");
                if (arr[2].equalsIgnoreCase(tableName)) {
                    return arr[0];
                }
                line = reader.readLine();
            }
            reader.close();
        } catch (Exception e) {
            System.out.println("Database not found. Please create database");
        }
        return null;
    }

    private void tokenizeTruncateQuery(Pattern pattern, Matcher matcher, String query) {
        pattern = Pattern.compile("(TRUNCATE)\\s+(TABLE)\\s+([\\w]+)\\s*$", Pattern.CASE_INSENSITIVE);
        matcher = pattern.matcher(query);

        if (matcher.find()) {
            String tableName = matcher.group(3);
            try {
                String location = getLocation(tableName);
                TruncateQueryExecutor truncateQueryExecutor = new TruncateQueryExecutor(tableName, databaseName, location);
                truncateQueryExecutor.performTruncateQueryOperation();
                writeDump(query);
            } catch (IOException e) {
                System.out.println("Cannot TRUNCATE table" + e.getMessage());
                e.getStackTrace();
            }
        } else {
            System.out.println("Query syntax is not correct, please check keywords spellings and order.");
        }
    }

    public static void main(String[] args) {
        QueryParser parser = new QueryParser();
        String query = "USE DATABASE DemoDB";
        parser.parsingQuery(query, false);
        query = "insert into students_1 (student_id,name,age,course,department_id) values (28,Anna,88,MACS,2)";
        parser.parsingQuery(query, false);
    }
}
