package edu.msu.cse.dkvf.clusterDesigner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.swing.JFrame;

public class ServerComponent extends Component {
	
	public ServerComponent(String id) {
		super("resources/server.png", id);
		parameters = new EditServerParametersFrame(id, false, this);
		parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		parameters.setTitle("Server Parameters - " + id);
	}
	
	public ServerComponent (ServerComponent that){
		super(that);
		parameters = new EditServerParametersFrame(id, (EditServerParametersFrame)that.parameters, this);
		parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		parameters.setTitle("Server Parameters - " + id);
	}
	
	public void save(ObjectOutputStream out) throws IOException{
		out.writeObject(id);
		out.writeInt(getX());
		out.writeInt(getY());
		((EditServerParametersFrame)parameters).save(out);

	}
	
	public static ServerComponent load (ObjectInputStream input) throws ClassNotFoundException, IOException{
		String id = (String)input.readObject();
		ServerComponent result = new ServerComponent(id);
		result.myX = input.readInt();
		result.myY = input.readInt();
		result.parameters = EditServerParametersFrame.load(input, id, false, result);
		result.parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		result.parameters.setTitle("Server Parameters - " + id);
		ClusterDesignerApplication.panel.add(result);
		result.setLocation(result.myX, result.myY);
		return result;
	}
	
}