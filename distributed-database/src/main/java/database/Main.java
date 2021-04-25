package database;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Scanner;

import org.json.JSONArray;
import org.json.JSONObject;

public class Main {

	public static void main(String[] args) {

		Scanner scanner = new Scanner(System.in);

		LoginUI loginUI = new LoginUI();
		loginUI.manageLoginUI(scanner);

		String query = "";
		boolean isExitQuery = false;

		while (!isExitQuery) {
			System.out.println("Enter your query or use exit command to exit from the database management system.");
			query = scanner.nextLine();

			if (query == null) {
				query = "";
			}

			if (query.trim().equalsIgnoreCase("exit")) {
				isExitQuery = true;
				continue;
			}

			if (query.contains("RECOVER")) {
				String[] recoverArray = query.split(" ");
				String filename = recoverArray[1] + "_dump.txt";
				executeRecovery("Database/" + filename);
			} else if (query.contains("ERD")) {
				String[] erdArray = query.split(" ");
				String filename = "Database/" + erdArray[1] + "_erd.txt";
				makeERD(erdArray[1], filename);
			} else {
				QueryParser queryParser = new QueryParser();
				boolean result = queryParser.parsingQuery(query, false);
			}

		}

	}

	public static void executeRecovery(String filePath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();
			while (line != null) {
				if (line.length() > 0) {
					QueryParser queryParser = new QueryParser();
					boolean result = queryParser.parsingQuery(line, false);
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (Exception e) {
			System.out.println("Error working with filesystem: " + e.getMessage());
		}

	}

	static void makeERD(String inputDatabase, String filename) {
		try {
			String erdData = "";
			BufferedReader reader = new BufferedReader(new FileReader("Database/meta.txt"));
			String line;
			while ((line = reader.readLine()) != null) {
				String s = readMetaFileRow(line, inputDatabase);
				if(s.trim().length() > 0) {
					erdData = erdData + s;
				}
			}
			FileWriter fstream = new FileWriter(filename, true);
            BufferedWriter out = new BufferedWriter(fstream);
            out.write(erdData);
            out.newLine();
            out.flush();
            out.close();
		} catch (Exception e) {
			System.out.println("Query Failed!");
		}
	}

	static String readMetaFileRow(String row, String inputDatabase) {
		String rowDetails = "";

		String[] rowElementsArray = row.split("@@@");
		String database = rowElementsArray[1];
		if (database != null && database.trim().equalsIgnoreCase(inputDatabase.trim())) {
			String tableName = rowElementsArray[2];
			rowDetails = rowDetails + "TableName: " + tableName + "\n";
			String primaryKey = rowElementsArray[3];
			rowDetails = rowDetails + "Primary Key: " + primaryKey + "\n";
			String columns = rowElementsArray[4];
			if (columns != null) {
				JSONObject columnsArrayObject = new JSONObject(columns);
				JSONArray columnsArray = new JSONArray(columnsArrayObject.get("columns").toString());
				if (columnsArray.length() > 0) {
					rowDetails = rowDetails + "Columns: \n";
					for (int i = 0; i < columnsArray.length(); i++) {
						JSONObject columnArrayObj = new JSONObject(columnsArray.get(i).toString());
						String columnType = columnArrayObj.getString("columnType");
						String columnName = columnArrayObj.getString("columnName");
						rowDetails = rowDetails + "ColumnName: " + columnName + " columnType: " + columnType + "\n";
					}
				} else {
					rowDetails = rowDetails + "Columns: No Columns\n";
				}
			} else {
				rowDetails = rowDetails + "Columns: No Columns\n";
			}

			String foreignKey = rowElementsArray[5];
			if (foreignKey != null) {
				JSONObject foreignKeyObj = new JSONObject(foreignKey);
				JSONArray foreignKeyArrays = new JSONArray(foreignKeyObj.get("keys").toString());
				if (foreignKeyArrays.length() > 0) {
					rowDetails = rowDetails + "Foreign Keys: \n";
					for (int i = 0; i < foreignKeyArrays.length(); i++) {
						JSONObject foreignKeyConstraintObj = new JSONObject(foreignKeyArrays.get(i).toString());
						String foreignKeyColumnInTable = foreignKeyConstraintObj.getString("column");
						String foreignKeyColumnName = foreignKeyConstraintObj.getString("foreignKeyColumn");
						String foreignKeyTableName = foreignKeyConstraintObj.getString("foreignKeyTable");
						rowDetails = rowDetails + "foreignKeyColumnInTable: " + foreignKeyColumnInTable
								+ " foreignKeyColumnName: " + foreignKeyColumnName + " foreignKeyTableName: "
								+ foreignKeyTableName + "\n";
					}
				} else {
					rowDetails = rowDetails + "Foreign Keys: No Foreign Keys\n";
				}
			} else {
				rowDetails = rowDetails + "Foreign Keys: No Foreign Keys\n";
			}

			rowDetails = rowDetails + "\n\n";
		}
		return rowDetails;
	}

}
