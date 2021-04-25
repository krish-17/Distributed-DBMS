package database;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class testClass {

    public void removeFromMetaFile(String inputTableName, String metaFileName) throws IOException {
        try {
            File file = new File("Database/" + metaFileName);
            File tempFile = new File("Database/" + "temp.txt");
            FileReader fr = new FileReader(file);
            BufferedReader br = new BufferedReader(fr);
            FileWriter fw = new FileWriter(tempFile);
            BufferedWriter bw = new BufferedWriter(fw);

            List<String> contents = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                String[] arr = line.split("@@@");
                String tableName = Arrays.asList(arr).get(2);
                if(tableName.trim().equals(inputTableName))
                {
                    System.out.println(line);
                    continue;
                }
                else{
                    contents.add(line);
                }
            }
            for(String i : contents)
            {
                bw.write(i);
                bw.newLine();
            }
            bw.close();
            file.delete();
            tempFile.renameTo(file);

        }catch (Exception e) {
            //System.out.println("Exception while removing from file");
        }
    }
}
