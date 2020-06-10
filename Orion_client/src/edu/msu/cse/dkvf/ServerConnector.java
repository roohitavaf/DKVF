package edu.msu.cse.dkvf;

import java.net.Socket;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import edu.msu.cse.dkvf.config.ConfigReader.ServerInfo;

/**
 * Class to connect to specified servers
 */
public class ServerConnector implements Runnable {
    ArrayList<ServerInfo> pendingServers;
    Map<String, CodedOutputStream> serversOut;
    Map<String, CodedInputStream> serversIn;
    Map<String, Socket> sockets;
    int sleepTime;
    Logger LOGGER;

    /**
     * Network operation status
     */
    public enum NetworkStatus {
        SUCCESS, FAILURE
    }

    /**
     * @param serversInfoToConnect The servers to connect to
     * @param serversOut           The map of IDs of servers to their output streams
     * @param serversIn            The map of IDs of servers to their input streams
     * @param sockets              The map of IDs to servers to their sockets
     * @param sleepTime            The sleep time before trying connection again
     * @param logger               The logger
     */
    public ServerConnector(ArrayList<ServerInfo> serversInfoToConnect, Map<String, CodedOutputStream> serversOut,
                           Map<String, CodedInputStream> serversIn, Map<String, Socket> sockets, int sleepTime, Logger logger) {
        this.LOGGER = logger;
        this.serversOut = serversOut;
        this.serversIn = serversIn;
        this.sockets = sockets;

        this.pendingServers = new ArrayList<>();
        for (ServerInfo si : serversInfoToConnect) {
            if (!serversIn.containsKey(si.id))
                pendingServers.add(si);
        }
        this.sleepTime = sleepTime;
    }

    /**
     * Tries to connect to specified servers. Sleeps before trying again.
     */
    public void run() {
        LOGGER.info("Server connector started.");
        // Periodically tries to connect to pending servers. Once successfully
        // connected,
        // add them to servers and remove them from pending servers.
        int i = 0;
        Socket newSocket = null;
        while (pendingServers.size() > 0) {
            try {
                newSocket = new Socket(pendingServers.get(i).ip, pendingServers.get(i).port);
                CodedOutputStream out = CodedOutputStream.newInstance(newSocket.getOutputStream());
                serversOut.put(pendingServers.get(i).id, out);
                CodedInputStream in = CodedInputStream.newInstance(newSocket.getInputStream());
                serversIn.put(pendingServers.get(i).id, in);
                LOGGER.fine(MessageFormat.format("Connected to server with\n\tid= {0} \n\tip= {1} \n\tport= {2}",
                        pendingServers.get(i).id,
                        pendingServers.get(i).ip,
                        String.valueOf(pendingServers.get(i).port)
                ));
                sockets.put(pendingServers.get(i).id, newSocket);
                pendingServers.remove(i);

            } catch (Exception e) {
                try {
                    if (newSocket != null)
                        newSocket.close();
                } catch (Exception e1) {

                }
            } finally {
                if (pendingServers.size() > 0) {
                    i = i++ % pendingServers.size();
                    if (i == pendingServers.size() - 1)
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            System.out.println("Problem in sleeping time in ServerConnector");
                            e.printStackTrace();
                        }
                }
            }
        }
        LOGGER.info("Sucessfully connected to all servers.");
    }

    /**
     * Is the connection to all expected servers are done.
     *
     * @return <b>true</b> The connection is done. <br/>
     * <b>false</b> The connection is not done yet.
     */
    public boolean isDone() {
        return pendingServers.size() == 0;
    }

}
