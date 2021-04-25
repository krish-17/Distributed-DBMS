package database;

import java.io.*;

public class TruncateQueryExecutor {
    private String databaseName;
    private String tableName;
    private String location;

    public TruncateQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }

    public void performTruncateQueryOperation() throws IOException {

        String temp = "myFile2.txt";

        File inputFile = new File( "Database/" + databaseName + '/' + tableName + ".txt");
        File tempFile = new File( "Database/" + databaseName + '/' + temp);

        FileReader fileReader = new FileReader(inputFile);
        FileWriter fileWriter = new FileWriter(tempFile);
        BufferedReader tableReader = new BufferedReader(fileReader);
        BufferedWriter tableWriter = new BufferedWriter(fileWriter);

        String columnHeaders = tableReader.readLine();
        tableWriter.write(columnHeaders);
        tableWriter.close();
        inputFile.delete();
        tempFile.renameTo(inputFile);
        System.out.println("Table values truncated");

        if(location.equalsIgnoreCase("remote")){
            RemoteFileHandler rhf = new RemoteFileHandler(databaseName, tableName);
            rhf.deleteObject();
        }
    }
}
