package edu.msu.cse.dkvf.clusterManager;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

public class Manager {
	private String errorMessage;
	private PrintStream out;
	
	public Manager (PrintStream out){
		this.out = out;
	}
	protected String getErrorMessage() {
		return errorMessage;
	}

	protected void setErrorMessage(Exception e) {
		errorMessage = exceptionToString(e);
	}
	
	protected  String exceptionToString (Exception e){
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		return sw.toString();
	}
	protected void setErrorMessage(String message, Exception e) {
		errorMessage = message + "\n"  + exceptionToString(e);
	}

	protected void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	protected void printMessageln(String message) {
		if (out != null)
			out.println(message);
	}
	
	protected void printMessage(String message) {
		if (out != null)
			out.print(message);
	}
	
	
}
