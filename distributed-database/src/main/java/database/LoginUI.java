package database;

import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;

public class LoginUI {

	public void manageLoginUI(Scanner scanner) {
		boolean isLoginPassed = false;
		while(isLoginPassed == false) {
			isLoginPassed = createLoginUI(scanner);
			if(isLoginPassed == false) {
				System.out.println("Please provide valid username and password");
			}
		}
		System.out.println("Successful login");
	}
	
	public boolean createLoginUI(Scanner scanner) {
		System.out.println("Please enter username");
		String username = scanner.next();
		System.out.println("Please enter password");
		String password = scanner.next();
		
		if(StringUtils.isBlank(username) && StringUtils.isBlank(password)) {
			return false;
		}
		
		Login login = new Login();
		password = login.decryptPassword(password);
		return login.verifyingCredentials(username, password);
	}
	
//	public static void main(String[] args) {
//		LoginUI loginUI = new LoginUI();
//		loginUI.manageLoginUI();
//	}
	
}
