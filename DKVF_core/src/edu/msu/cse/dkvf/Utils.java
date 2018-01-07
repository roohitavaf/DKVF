package edu.msu.cse.dkvf;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.logging.Level;

/**
 * General utilities
 *
 */
public class Utils {
	
	/** 
	 * Converts string to log level
	 * @param level log level string
	 * @return log level
	 */
	public static Level getLevelFromString(String level) {
		return Level.parse(level.toUpperCase());
	}
	/**
	 * Creates the directory if it does not exist.
	 * @param address
	 * 			The address of the directory
	 */
	public static void checkAndCreateDir(String address) {
		File files = new File(address);
		if (!files.exists()) {
			files.mkdirs();
		}
	}
	
	/**
	 * Checks the parent of a path to a file, and create the parent path if it does not exist.
	 * @param fileAddress
	 * 			The address of the file
	 */
	public static void checkParentAndCreate (String fileAddress){
		File file = new File(fileAddress);
		if (file.getParent() != null)
			checkAndCreateDir(file.getParent());
	}
	
	/**
	 * Creates of clean a directory
	 * @param address The address of the directory
	 * @throws IOException
	 */
	public static void createOrClearDir(String address) throws IOException {
		File file = new File(address);
		if (file.exists())
			delete(file);
		file.mkdirs();
	}

	/**
	 * Deletes a file.
	 * @param file The address of the file. 
	 * @throws IOException
	 */
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
	
	/**
	 * Converts a string to long value by hashing.
	 * @param strToHash The string to hash
	 * @return The long value of the hash code
	 * @throws NoSuchAlgorithmException
	 */
	public static long getMd5HashLong (String strToHash) throws NoSuchAlgorithmException{
		byte[] bytesOfMessage = strToHash.getBytes();
		MessageDigest md = MessageDigest.getInstance("MD5");
		byte[] md5 = md.digest(bytesOfMessage);
		//we only use 8 bytes of the generated md5. 
		long l = ((md5[0] & 0xFFL) << 56) |
		         ((md5[1] & 0xFFL) << 48) |
		         ((md5[2] & 0xFFL) << 40) |
		         ((md5[3] & 0xFFL) << 32) |
		         ((md5[4] & 0xFFL) << 24) |
		         ((md5[5] & 0xFFL) << 16) |
		         ((md5[6] & 0xFFL) <<  8) |
		         ((md5[7] & 0xFFL) <<  0) ;
		
		return  Math.abs(l);
	}
	
	/**
	 * Creates a string message of an exception object.
	 * @param e The Exception object to translate 
	 * @return The string message
	 */
	public static  String exceptionToString (Exception e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}

	/**
	  * Creates a string message of an exception object with an additional log message.
	 * @param message The additional log message
	 * @param e The Exception object to translate
	 * @return The string message
	 */
	public static String exceptionLogMessge (String message, Exception e){
		return MessageFormat.format("{0}. Stack trace:\n{1}", message, exceptionToString(e));
	}
	
}
