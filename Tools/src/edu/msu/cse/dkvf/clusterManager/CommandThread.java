package edu.msu.cse.dkvf.clusterManager;

import java.io.IOException;
import java.io.PrintStream;
import java.text.MessageFormat;

import com.jcraft.jsch.JSchException;

import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentReader.YcsbClientNode;

/**
 * Run a command in a separate thread.
 *
 */
public class CommandThread extends Thread{

	private Exception e;

	private String command;
	private YcsbClientNode node;
	private boolean strictHostKeyChecking;
	private PrintStream out;

	public void init(String command, YcsbClientNode node, boolean strictHostKeyChecking, PrintStream out) {
		this.command = command;
		this.node = node;
		this.strictHostKeyChecking = strictHostKeyChecking;
		this.out = out;
	}

	public void run() {

		// connect to server
		try {
			node.connect(strictHostKeyChecking);
		} catch (Exception e) {
			this.e = e;
			return;
		}
		
		//running the command:
		try {
			node.sshManager.sendCommandGetResponse(command);
			
			//printMessage(MessageFormat.format("Finished command at client {0}:{1}", node.id, node.ip));
		} catch (JSchException | IOException  e) {
			//printMessage(MessageFormat.format("Failed to perform command at client {0}:{1}", node.id, node.ip));
			this.e = e;
			return;
		}

	}

	public Exception getError() {
		return e;
	}

	private void printMessage(String str) {
		if (out != null)
			out.println(str);
	}

}
