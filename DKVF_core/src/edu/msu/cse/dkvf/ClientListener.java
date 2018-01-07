package edu.msu.cse.dkvf;

import java.net.ServerSocket;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.logging.Logger;

/**
 * The listener for incoming clients
 *
 */
public class ClientListener implements Runnable {
	/**
	 * The port to listen for clients
	 */
	int port;
	
	/**
	 * The protocol to run its client handler upon receiving a client message
	 */
	DKVFServer protocol;
	
	/**
	 * The logger to use by client listener
	 */
	Logger LOGGER;

	/**
	 * Constructor for ClientListener
	 * @param port The port to listen for incoming clients
	 * @param protocol The Protocol object that is used to handle client requests
	 * @param logger The logger
	 */
	public ClientListener(int port, DKVFServer protocol, Logger logger) {
		this.LOGGER = logger;
		this.port = port;
		this.protocol = protocol;
	}

	@Override
	/**
	 * Listens for clients, and creates one thread for each client. 
	 */
	public void run() {
		LOGGER.info("Start listening for clients at port: " + port);
		ServerSocket serverSocket = null;
		try {
			serverSocket = new ServerSocket(port);
		} catch (Exception e) {
			try {
				LOGGER.severe(MessageFormat.format(
						"Problem in creating server socket to accept clients at port= {0} toString: {1} Message:\n{2}",
						port, e.toString(), " Message: " + e.getMessage()));
				if (serverSocket != null)
					serverSocket.close();
			} catch (Exception e1) {
				LOGGER.warning(MessageFormat.format(
						"Problem in closing serverSocket to accept clients at port= {0} toString: {1} Message:\n{2}",
						port, e1.toString(), " Message: " + e1.getMessage()));

			}
			return;
		}

		while (true) {
			try {
				Socket clientSocket = serverSocket.accept();
				LOGGER.finer("New client arrived.");
				protocol.incrementNumberOfClients();
				ClientHandler ch = new ClientHandler(clientSocket, protocol, LOGGER);
				Thread t = new Thread(ch);
				t.start();
			} catch (Exception e) {
				LOGGER.severe(MessageFormat.format(
						"Problem in accepting new client socket at port= {0} toString: {1} Message:\n{2}", port,
						e.toString(), " Message: " + e.getMessage()));
			}

		}
	}

}
