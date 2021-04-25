package database;

import java.io.*;
import java.util.*;

public class DistributedSystem {

    public void distributedDatabaseActivity(String tableName) {
        try {
            File inputFile = new File("meta.txt"); //meta.txt
            String outputFile = "GDD.txt";

            FileReader fileReader = new FileReader(inputFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            FileWriter fileWriter = new FileWriter(outputFile, true);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);

            String currentLine;
            while ((currentLine = bufferedReader.readLine()) != null) {
                String[] line = currentLine.split("@@@");
                List<String> list = Arrays.asList(line);
                for (int i = 0; i < list.size(); i++) {
                    if (list.get(i).contains(tableName)) {
                        String output = tableName + " table belongs to " + list.get(0) + " database";
                        System.out.println(output);
                        bufferedWriter.write(output);
                        bufferedWriter.newLine();
                        //bufferedWriter.flush();
                    }
                }
            }
            bufferedWriter.close();
        } catch(IOException ioException) {
            ioException.getStackTrace();
        }
    }
    public static void main(String[] args){
        DistributedSystem ds = new DistributedSystem();
        ds.distributedDatabaseActivity("Persons_1");
    }
}

