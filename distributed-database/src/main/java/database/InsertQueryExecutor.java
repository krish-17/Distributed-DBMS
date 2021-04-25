package database;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class InsertQueryExecutor {
    private String tableName;
    private String databaseName;
    private String location;
    private static final Object REMOTE_URL = "https://storage.googleapis.com/5408_project_team6/Database";

    public InsertQueryExecutor(String tableName, String databaseName, String location) {
        this.tableName = tableName;
        this.databaseName = databaseName;
        this.location = location;
    }
    public void performInsertQueryOperation(String[] values) throws IOException
    {
        try {
            File file = new File("Database" + "/" + databaseName + '/' + tableName + ".txt");
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter tableWriter = new BufferedWriter(fileWriter);
            List<String> list = Arrays.asList(values);
            tableWriter.write(String.join("$", list));
            tableWriter.newLine();
            tableWriter.flush();
            tableWriter.close();
            if(location.equalsIgnoreCase("remote"))
            {
                RemoteFileHandler rhf = new RemoteFileHandler(databaseName, tableName);
                rhf.uploadObject();
            }
        }
        catch(IOException e) {
            e.getStackTrace();
        }
    }
}
