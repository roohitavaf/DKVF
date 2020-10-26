package edu.msu.cse.dkvf.clusterManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import java.util.logging.Level;

public class Utils {
	public static Level getLevelFromString(String level) {
		if (level.equals("severe"))
			return Level.SEVERE;
		else if (level.equals("warning"))
			return Level.WARNING;
		else if (level.equals("info"))
			return Level.INFO;
		else if (level.equals("fine"))
			return Level.FINE;
		else if (level.equals("finer"))
			return Level.FINER;
		else if (level.equals("finest"))
			return Level.FINEST;
		else if (level.equals("all"))
			return Level.ALL;
		else if (level.equals("config"))
			return Level.CONFIG;
		else
			return Level.OFF;
	}

	public static void createTextFile(String content, String address) throws FileNotFoundException, UnsupportedEncodingException {
		try {
		    Files.createFile(Paths.get(address));
		} catch (IOException ignored) {
		}
		PrintWriter writer = new PrintWriter(address, "UTF-8");
		writer.print(content);
		writer.close();
	}

	public static void checkAndCreateDir(String address) {
		File files = new File(address);
		if (!files.exists()) {
			files.mkdirs();
		}
	}

	public static void createOrClearDir(String address) throws IOException {
		File file = new File(address);
		if (file.exists())
			delete(file);
		file.mkdirs();
	}

	public static void delete(File file) throws IOException {

		if (file.isDirectory()) {

			// directory is empty, then delete it
			if (file.list().length == 0) {

				file.delete();

			} else {

				// list all the directory contents
				String files[] = file.list();

				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(file, temp);

					// recursive delete
					delete(fileDelete);
				}

				// check the directory again, if empty then delete it
				if (file.list().length == 0) {
					file.delete();
				}
			}

		} else {
			// if file, then delete it
			file.delete();
		}
	}

	public static void checkParentAndCreate(String fileAddress) {
		File file = new File(fileAddress);
		if (file.getParent() != null)
			checkAndCreateDir(file.getParent());
	}

	static String fixWindowsPath(String path) {
		if (path.startsWith("/cygdrive/")) {
			String[] pathParts = path.split("/");
			String drive = pathParts[2];
			return drive + ":/" + path.substring(path.indexOf("/" + drive + "/") + 3);
		}
		return path;
	}

	public static boolean isNumeric(String str) {
		return str.matches("-?\\d+(\\.\\d+)?"); // match a number with optional
												// '-' and decimal.
	}
	
	public static String getPadding (int n){
		String spaces = String.format("%"+n+"s", "");
		return spaces;
	}
}
