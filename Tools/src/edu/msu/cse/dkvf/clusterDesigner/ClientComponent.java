package edu.msu.cse.dkvf.clusterDesigner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import javax.swing.JFrame;

public class ClientComponent extends Component {

	public ClientComponent(String id) {
		super("resources/client.png", id);
		parameters = new EditExperimentParametersFrame(id, false, this);
		parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		parameters.setTitle("Experiment Parameters - " + id);
	}
	

	public ClientComponent (ClientComponent that){
		super(that);
		parameters = new EditExperimentParametersFrame(id, (EditExperimentParametersFrame)that.parameters, this);
		parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		parameters.setTitle("Experiment Parameters - " + id);
	}

	public void save(ObjectOutputStream out) throws IOException{
		out.writeObject(id);
		out.writeInt(getX());
		out.writeInt(getY());
		((EditExperimentParametersFrame)parameters).save(out);

	}
	
	public static ClientComponent load (ObjectInputStream input) throws ClassNotFoundException, IOException{
		String id = (String)input.readObject();
		ClientComponent result = new ClientComponent(id);
		result.myX = input.readInt();
		result.myY = input.readInt();
		result.parameters = EditExperimentParametersFrame.load(input, id, false, result);
		result.parameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		result.parameters.setTitle("Experiment Parameters - " + id);
		ClusterDesignerApplication.panel.add(result);
		result.setLocation(result.myX, result.myY);
		return result;
	}
	
}