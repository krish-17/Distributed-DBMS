package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.json.JSONObject;

public class CreateTableQuery {

    public void exceuteCreateTableQuery(String database, String tableName, String primaryKey, List<String> columns,
                                        JSONObject tableColumnsObject, JSONObject tableForeignKeysObject) {
        String server = createFileInFileSystem(columns,database, tableName);
        if (server.trim().equalsIgnoreCase("remote") || server.trim().equalsIgnoreCase("local")) {
            createRecordInMetaDataFile(server, database, tableName, primaryKey, tableColumnsObject, tableForeignKeysObject);
        }
    }

    private void createRecordInMetaDataFile(String server, String database, String tableName, String primaryKey,
                                            JSONObject tableColumnsObject, JSONObject tableForeignKeysObject) {
        try {
            File f = new File("Database" + "/" + "meta.txt");
            FileWriter fstream = new FileWriter(f, true);
            BufferedWriter out = new BufferedWriter(fstream);
            String s = server + "@@@" + database + "@@@" + tableName + "@@@" + primaryKey + "@@@" + tableColumnsObject.toString()
                    + "@@@" + tableForeignKeysObject.toString();
            out.write(s);
            out.newLine();
            out.flush();
            out.close();
            
            RemoteFileHandler remoteFileHandler = new RemoteFileHandler("Database", "meta");
            remoteFileHandler.uploadObject();
        } catch (Exception e) {
            System.out.println("Exception while writing into file");
        }
    }

    private String createFileInFileSystem(List<String> columns, String database, String tableName) {
    	String server = "";
        File tableFile = new File("Database/" + database + "/" + tableName + ".txt");
        if (!tableFile.exists()) {
            try {
            	FileWriter fstream = new FileWriter(tableFile, true);
                BufferedWriter out = new BufferedWriter(fstream);
                String s = "";
                for (String string : columns) {
					s = s + string + "$"; 
				}
                s = s.substring(0, s.length() -1);
                out.write(s);
                out.newLine();
                out.flush();
                out.close();
                
                Random rand = new Random();
                int num = rand.nextInt();
                
                
                if(num % 2 == 0) {
                	RemoteFileHandler remoteFileHandler = new RemoteFileHandler(database, tableName);
                    remoteFileHandler.uploadObject();
                	server = "remote";
                } else {
                	server = "local";
                }
                  
            } catch (IOException e) {
                System.out.println(e);
                System.out.println("Exception while persisting data. Please contact admin."); 
            }
        } else {
            System.out.println("Table already exists");
        }
        return server;
    }

}
