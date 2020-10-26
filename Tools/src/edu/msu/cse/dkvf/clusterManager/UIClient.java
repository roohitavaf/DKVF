package edu.msu.cse.dkvf.clusterManager;

import java.text.MessageFormat;
import java.util.Scanner;


public class UIClient {
	ClientManager cm; 
	String serverId;
	
	public UIClient (ClientManager cm, String serverId){
		this.cm = cm;
		this.serverId = serverId;
	}
	
	public void runUI(Scanner scanner){
		String cursor = MessageFormat.format("Server:{0}> ", serverId);
		String command = "";
		while (!command.equals("exit")) {
			System.out.print(cursor);
			command = scanner.nextLine().trim();
	
			//System.out.println("Reading from consol");
			//command = System.console().readLine();
			String[] commandParts = command.split("\\s+");
			switch (commandParts[0]) {
			case "put":
				put(commandParts);
				break;
			case "get":
				get(commandParts);
				break;
			case "read_error":
				readError();
				break;
			case "exit":
				exit();
				break;
			case "":
				break;
			default:
				System.out.println("Not recognized " + commandParts[0] + ". Type help for help.");
			}
		}
	}

	private void exit() {
		
	}

	private void get(String[] commandParts) {
		if (commandParts.length < 2) {
			System.out.println("Please specify the key to read.");
			return;
		}
		byte[] ss = cm.get(commandParts[1]);
		if (ss != null)
			System.out.println(MessageFormat.format("value: {0}", new String(ss)));
		else 
			System.out.println("key not found");
		if (ss == null) {
			System.out.println("Problem getting. Type \"read_error\" to see the error.");
			return;
		}
	}

	private void put(String[] commandParts) {
		if (commandParts.length < 3) {
			System.out.println("Please specify the key and its value to put.");
			return;
		}
		boolean ss = cm.put(commandParts[1], commandParts[2].getBytes());
		if (!ss) {
			System.out.println("Problem putting. Type \"read_error\" to see the error.");
			return;
		}
		
	}
	
	private  void readError() {
		if (cm.getErrorMessage() != null)
			System.out.println(cm.getErrorMessage());
		else
			System.out.println("No error message exists to show.");

	}
}
