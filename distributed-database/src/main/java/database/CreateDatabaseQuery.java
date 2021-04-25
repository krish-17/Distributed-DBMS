package database;

import java.io.File;

public class CreateDatabaseQuery {

    public void exceuteCreateDatabaseQuery(String database) {
        File databaseFolder = new File("Database" + "/" + database);
        if (!databaseFolder.exists()) {
            databaseFolder.mkdirs();
        }
    }

    public static void main(String[] args) {
        System.out.println("Started");
        CreateDatabaseQuery CreateDatabaseQuery = new CreateDatabaseQuery();
        CreateDatabaseQuery.exceuteCreateDatabaseQuery("test3");
    }

}
