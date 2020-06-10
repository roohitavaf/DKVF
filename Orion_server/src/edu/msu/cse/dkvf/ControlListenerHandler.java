package edu.msu.cse.dkvf;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.MessageFormat;
import java.util.logging.Logger;

import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.ControlMessage;
import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.ControlReply;
import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.StatusCheckReply;
import edu.msu.cse.dkvf.controlMetadata.ControlMetadata.TurnoffReply;

/**
 * The listener and handler for incoming control messages
 */
public class ControlListenerHandler implements Runnable {
    int port;
    DKVFServer protocol;
    Logger LOGGER;

    /**
     * Constructor for ControlListenerHandler
     *
     * @param port     The port to listen for incoming control messages
     * @param protocol The Protocol object to handle extra control messages
     * @param logger   The logger
     */
    public ControlListenerHandler(int port, DKVFServer protocol, Logger logger) {
        this.LOGGER = logger;
        this.port = port;
        this.protocol = protocol;
    }

    /**
     * Runs the thread to listen for incoming control messages. This method handles turn-off messages by itself.
     */
    public void run() {
        // It will listen on the control port.
        LOGGER.info("Start listening for control requests at port: " + port);

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (Exception e) {
            try {
                LOGGER.severe(MessageFormat.format("Problem in creating server socket to accept control requests at port= {0} toString: {1} Message:\n{2}", port, e.toString(), " Message: " + e.getMessage()));
                serverSocket.close();
            } catch (Exception e1) {
                LOGGER.warning(MessageFormat.format("Problem in closing server socket to accept control requests = at port= {0} toString: {1} Message:\n{2}", port, e.toString(), " Message: " + e.getMessage()));
            }
            return;
        }
        InputStream in = null;
        OutputStream out = null;
        ControlMessage cm = null;
        Socket clientSocket = null;
        while (true) {
            try {
                clientSocket = serverSocket.accept();
                in = clientSocket.getInputStream();
                out = clientSocket.getOutputStream();
                cm = ControlMessage.parseDelimitedFrom(in);
                if (cm == null)
                    return;
            } catch (Exception e) {
                LOGGER.severe(MessageFormat.format("Problem in accepting new control request at port= {0} toString: {1} Message:\n{2}", port, e.toString(), " Message: " + e.getMessage()));
            }
            try {
                LOGGER.finer(MessageFormat.format("New control message arrived:\n{0}", cm.toString()));
                if (cm.hasTurnoff())
                    turnoff(out, clientSocket);
                else {
                    ControlReply cr = handleControlMessage(cm);
                    if (cr != null)
                        cr.writeDelimitedTo(out);
                    clientSocket.close();
                }
            } catch (Exception e) {

                LOGGER.severe(Utils.exceptionLogMessge("Problem in sending control respond", e));
            }
        }
    }

    /**
     * Handles control messages other than turn off.
     * It calls {@link DKVFServer#handleControlMessage} of the protocol if the control message is unknown.
     *
     * @param cm The received control message.
     * @return The control reply to send to the sender of control message.
     */
    private ControlReply handleControlMessage(ControlMessage cm) {
        if (cm.hasStatusCheck())
            return statusCheck();
        else
            return protocol.handleControlMessage(cm);
    }

    /**
     * Turns off the server.
     *
     * @param out    The output stream of sender of the control message
     * @param socket The socket of the sender of the control message
     */
    private void turnoff(OutputStream out, Socket socket) {
        // TODO Auto-generated method stub
        // should turn off gracefully, store db, relsease ....
        if (protocol.prepareToTurnOff()) {
            ControlReply cr = ControlReply.newBuilder().setTrunoffReply(TurnoffReply.newBuilder().setOk(true).build()).build();
            try {
                cr.writeDelimitedTo(out);
                socket.close();
                LOGGER.info("Server is going to turn off as requested...");
                System.exit(0);
            } catch (IOException e) {
                LOGGER.severe("Failed to send turnoff reply message to requester.");
            }
        } else {
            LOGGER.info("Request to turnoff the server got rejected.");
            ControlReply cr = ControlReply.newBuilder().setTrunoffReply(TurnoffReply.newBuilder().setOk(false).build()).build();
            try {
                cr.writeDelimitedTo(out);
            } catch (IOException e) {
                LOGGER.severe("Failed to send turnoff reply message to requester.");
            }
        }
    }

    /**
     * Prepares the status report of this node
     *
     * @return The control reply contacting the status report
     */
    private ControlReply statusCheck() {
        int numOfClients = protocol.getNumberOfClients();
        int numOfServers = protocol.getNumberOfServers();
        int numOfExpectedServers = protocol.getNumOfExpectedServers();
        String id = protocol.getId();

        StatusCheckReply scr = StatusCheckReply.newBuilder().setClients(numOfClients).setServers(numOfServers).setServersExpected(numOfExpectedServers).setId(id).build();

        ControlReply cr = ControlReply.newBuilder().setStatusCheckReply(scr).build();
        return cr;
    }

}
