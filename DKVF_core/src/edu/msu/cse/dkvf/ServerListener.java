package edu.msu.cse.dkvf;


import java.net.ServerSocket;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * The listener for incoming servers
 *
 */
public class ServerListener implements Runnable {
	
	/**
	 * Port to listen for servers
	 */
	int port;
	
	/**
	 * The protocol to run its server handler upon receiving a server message
	 */
	DKVFServer protocol;
	
	/**
	 * The logger to use by server listener
	 */
	Logger LOGGER;
	
	/**
	 * Constructor for ServerListener
	 * @param port The port number to listen for incoming server messages
	 * @param protocol The Protocol object to call for handling server messages
	 * @param logger THe logger
	 */
	public ServerListener(int port, DKVFServer protocol, Logger logger) {
		this.LOGGER = logger;
		this.port = port;
		this.protocol = protocol;
	}

	/**
	 * Listens for servers, and creates one thread for each server. 
	 */
	public void run() {
		// It will listen on the server port, and create one thread per server.
		LOGGER.info("Start listening for servers at port: " + port);

		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(port);
		} catch (Exception e) {
			try {
				LOGGER.severe(MessageFormat.format(
						"Problem in creating server socket to accept server at port= {0} toString: {1} Message:\n{2}",
						port, e.toString(), " Message: " + e.getMessage()));
				serverSocket.close();
			} catch (Exception e1) {
				LOGGER.warning(MessageFormat.format(
						"Problem in closing server socket to accept server at port= {0} toString: {1} Message:\n{2}",
						port, e.toString(), " Message: " + e.getMessage()));
			}
			return;
		}

		while (true) {
			try {
				Socket clientSocket = serverSocket.accept();
				ServerHandler sh = new ServerHandler(clientSocket, protocol, LOGGER);
				LOGGER.finer("New server arrived.");
				protocol.incrementNumberOfServers();
				Thread t = new Thread(sh);
				t.start();
			} catch (Exception e) {
				LOGGER.severe(MessageFormat.format(
						"Problem in accepting new server socket at port= {0} toString: {1} Message:\n{2}", port,
						e.toString(), " Message: " + e.getMessage()));
			}
		}
	}

}
