package database;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class DeleteQueryExecutor {

    private String tableName;
    private String databaseName;
    private String location;

    public DeleteQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }

    public void splitCondition(String condition) //Id=2
    {
        try {
            String operator = fetchOperation(condition);
            String[] values = condition.split(operator);
            List<String> records = new ArrayList<>();
            for(int i=0; i< values.length; i++) {
                records.add(values[i]);
            }
            String column_name = records.get(0);
            String column_value = records.get(1);
            performDeleteQueryOperation(column_name,column_value,operator);

        } catch(Exception e){
            e.getStackTrace();
        }
    }

    private String fetchOperation(String condition) {
        if (condition.contains(">=")) {
            return ">=";
        }
        if (condition.contains("!=")) {
            return "!=";
        }
        if (condition.contains("<=")) {
            return "<=";
        }
        if (condition.contains("=")) {
            return "=";
        }
        if (condition.contains(">")) {
            return ">";
        }
        if (condition.contains("<")) {
            return "<";
        }
        return "";
    }

    public void performDeleteQueryOperation(String column_name, String column_value, String operator) throws IOException
    {
        String temp = "myFile2.txt";

        File inputFile = new File("Database/" + databaseName + '/' + tableName + ".txt");
        File tempFile = new File("Database/" + databaseName + '/' + temp);

        BufferedReader tableReader = new BufferedReader(new FileReader(inputFile));
        BufferedWriter tableWriter = new BufferedWriter(new FileWriter(tempFile));

        if ((column_name != null) && (column_value != null)) {
            try {
                String line = tableReader.readLine();
                int lineIndex = 0;
                int columnIndex = findIndex(line, column_name);

                List<String[]> records = new ArrayList<>();
                records.add(line.split("\\$"));
                while ((line = tableReader.readLine()) != null) {
                    lineIndex++;
                    String[] columns = line.split("\\$");
                    if(operator.equals("=")) {
                        if (columns[columnIndex].trim().equals(column_value)) {
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                    else if(operator.equals(">"))
                    {
                        if(Integer.parseInt(columns[columnIndex]) > Integer.parseInt(column_value)){
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                    else if(operator.equals(">="))
                    {
                        if(Integer.parseInt(columns[columnIndex]) >= Integer.parseInt(column_value)){
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                    else if(operator.equals("<"))
                    {
                        if(Integer.parseInt(columns[columnIndex]) < Integer.parseInt(column_value)){
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                    else if(operator.equals("<="))
                    {
                        if(Integer.parseInt(columns[columnIndex]) <= Integer.parseInt(column_value)){
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                    else if(operator.equals("!="))
                    {
                        if(Integer.parseInt(columns[columnIndex]) != Integer.parseInt(column_value)){
                            continue;
                        }
                        else{
                            records.add(columns);
                        }
                    }
                }
                for(String[] i : records)
                {
                    tableWriter.write(String.join("$",i));
                    tableWriter.newLine();
                }
                tableWriter.close();
                inputFile.delete();
                tempFile.renameTo(inputFile);

                if(location.equalsIgnoreCase("remote")){
                    RemoteFileHandler rhf = new RemoteFileHandler(databaseName, tableName);
                    rhf.uploadObject();
                }
            } catch (IOException e) {
                e.getStackTrace();
            }
        } else {
            String line = tableReader.readLine();

            List<String[]> records = new ArrayList<>();
            records.add(line.split("\\$"));
            for(String[] i : records)
            {
                tableWriter.write(String.join("$",i));
            }
            tableWriter.close();
            inputFile.delete();
            tempFile.renameTo(inputFile);
        }
    }

    public int findIndex(String line, String column_name){
        int columnIndex = 0;
        String[] columnHeaders = line.split("\\$");
        for(String value:columnHeaders) {
            if(value.trim().equals(column_name)) {
                break;
            }
            else {
                columnIndex++;
            }
        }
        return columnIndex;
    }
}


