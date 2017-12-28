package edu.msu.cse.dkvf;

import java.text.MessageFormat;
import java.util.logging.Logger;

import com.google.protobuf.CodedOutputStream;

import edu.msu.cse.dkvf.metadata.Metadata.ClientMessage;
import edu.msu.cse.dkvf.metadata.Metadata.ClientReply;

/**
 * The class to facilitate the communicate with client for the server side of the
 * protocol
 * 
 *
 */
public class ClientMessageAgent {
	ClientMessage cm;
	Logger LOGGER;
	CodedOutputStream out;
	
	/**
	 * Constructor for ClientMEssageAgent
	 * @param cm The client message
	 * @param out The output stream for sending the reply to the client
	 * @param logger THe logger
	 */
	public ClientMessageAgent(ClientMessage cm, CodedOutputStream out, Logger logger) {
		this.cm = cm;
		this.LOGGER = logger;
		this.out = out;

	}

	/**
	 * Gets the received client message. 
	 * @return
	 * 			The received client message
	 */
	public ClientMessage getClientMessage() {
		return cm;
	}

	/**
	 * Sends response to the client message.
	 * @param cr
	 * 			The client reply to send to client. 
	 */
	public void sendReply(ClientReply cr) {
		try {
			
			out.writeInt32NoTag(cr.getSerializedSize());
			cr.writeTo(out);
			out.flush();
			//cr.writeDelimitedTo(out);
			LOGGER.finer(MessageFormat.format("Sent to client: \n Client message={0}\n Response= {1}", cm.toString(), cr.toString()));
		} catch (Exception e) {
			LOGGER.severe(MessageFormat.format("Problem in sending clienr response. toString={0}, Message={1}", e.toString(), e.getMessage()));

		}
	}

}
