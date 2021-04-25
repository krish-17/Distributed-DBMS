package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Login {

	public String decryptPassword(String password) {
		int decryptionNumber = password.length();
		int lengthOfDecryptionNumber = Integer.toString(decryptionNumber).length();
		String decryptionString = lengthOfDecryptionNumber + "" + decryptionNumber + password + decryptionNumber;
		return decryptionString;
	}

	private String encryptPassword(String password) {
		int i = Integer.parseInt(Character.toString(password.charAt(0)));
		password = password.substring(i + 1, password.length() - i);
		return password;
	}

	public boolean verifyingCredentials(String inputUsername, String decryptedPassword) {
		boolean areCredentialsMatched = false;
		String inputPassword = encryptPassword(decryptedPassword);
		Scanner scanner = null;
		try {
			File file = new File("Credentials.txt");
			scanner = new Scanner(file);
			while (scanner.hasNextLine()) {
				String nextLine = scanner.nextLine();
				if ((nextLine != null) && (nextLine.length() > 0)) {
					String[] credentials = nextLine.split("@@@");
					String username = credentials[0];
					String password = credentials[1];
					if (username.trim().equalsIgnoreCase(inputUsername)
							&& password.trim().equalsIgnoreCase(inputPassword)) {
						areCredentialsMatched = true;
					}
				}
			}
		} catch (FileNotFoundException e) {
			System.out.println("File is not found in the system");
		} finally {
			scanner.close();
		}
		return areCredentialsMatched;
	}

	public static void main(String[] args) {
		Login login = new Login();
		String decrypted = login.decryptPassword("password");
		System.out.println("de " + decrypted);
		String encrypted = login.encryptPassword(decrypted);

		System.out.println(encrypted);

	}
}
