package database;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;


public class TransactionManager {

    private static final String BEGIN = "BEGIN TRANSACTION";
    private static final String ROLLBACK = "ROLLBACK";
    private static final String SAVEPOINT = "SAVEPOINT";
    private static final String COMMIT = "COMMIT";



    long transactionId;
    String databaseName;

    List<String> transactionList;

    public TransactionManager(String databaseName) {
        transactionId = System.currentTimeMillis();
        this.databaseName = databaseName;
    }


    void addTransaction(String transactionStatement) {
        try {
            String filePath = databaseName + "/" + String.valueOf(transactionId) + ".txt";
            FileWriter tempTransWriter = new FileWriter(filePath);
            tempTransWriter.write(transactionStatement);
            tempTransWriter.write("\n");
            tempTransWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void endAndExecuteTransaction() {
        try {
            String filePath = databaseName + "/" + transactionId + ".txt";
            BufferedReader metaReader = new BufferedReader(new FileReader(filePath));
            String rows = null;
            transactionList = new ArrayList<>();
            while ((rows = metaReader.readLine()) != null) {
                transactionList.add(rows);
            }
            List<String> queryToBeExecutedList = queryThatIsExecutedInTransaction(transactionList);
            QueryParser qp = new QueryParser();
            for (String query : queryToBeExecutedList) {
                qp.parsingQuery(query, false);
            }
            File file =  new File(filePath);
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private List<String> queryThatIsExecutedInTransaction(List<String> transactionList) {
        List<String> executableQueries = new ArrayList<>();
        int currentSavePointIndex = -1;
        for (String query :  transactionList) {
            if (query.contains(SAVEPOINT)) {
                executableQueries.add(SAVEPOINT);
                currentSavePointIndex = executableQueries.size();
            }
            else if (query.contains(ROLLBACK)) {
                if (currentSavePointIndex != -1) {
                    int lastIndexOfExecutableQuery = executableQueries.size()-1;
                    while (!executableQueries.get(lastIndexOfExecutableQuery).equalsIgnoreCase(SAVEPOINT)) {
                        executableQueries.remove(lastIndexOfExecutableQuery);
                        lastIndexOfExecutableQuery--;
                    }
                    executableQueries.remove(lastIndexOfExecutableQuery);
                    currentSavePointIndex = -1;
                } else {
                    throw new IllegalArgumentException("Improper Rollback occurred");
                }
            }
            else if(query.contains(COMMIT)) {
                break;
            }
            else {
                if(!query.equalsIgnoreCase(BEGIN))
                    executableQueries.add(query);
            }
        }
        if (currentSavePointIndex != -1) {
            executableQueries.remove(SAVEPOINT);
        }
        return executableQueries;
    }

}
