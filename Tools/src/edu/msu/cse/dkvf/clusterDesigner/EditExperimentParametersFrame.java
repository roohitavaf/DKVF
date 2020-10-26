package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.EventQueue;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Property;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Parameters;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Parameters.ProtocolProperties;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Storage;

import javax.swing.JLabel;
import javax.swing.JTextField;
import java.awt.GridLayout;
import javax.swing.SwingConstants;

import javax.swing.JButton;
import javax.swing.BoxLayout;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

public class EditExperimentParametersFrame extends JFrame {

	private JPanel contentPane;
	private JTextField clientClassNameTextField;
	private JTextField clientJarFileTextField;
	private JTextField usernameTextField;
	private JLabel lblUsername;
	private JTextField passwordTextField;
	private JLabel lblPassword;
	private JTextField keyTextField;
	private JLabel lblKey;
	private JTextField workingDirectoryTextField;
	private JTextField connectorSleepTImeTextField;
	private JTextField osTextField;
	private JTextField protocolLogFileTextField;
	private JTextField protocolLogLevelTextField;
	private JTextField protocolLogTypeTextField;
	private JTextField protocolStdLogLevelTextField;
	private JTextField frameworkLogFileTextField;
	private JLabel lblWorkingdirectory;
	private JLabel lblConnectorsleeptime;
	private JLabel lblChannelcapacity;
	private JLabel lblProtocollogfile;
	private JLabel lblProtocolloglevel;
	private JLabel lblProtocollogtype;
	private JLabel lblProtocolstdloglevel;
	private JLabel lblFrameworklogfile;
	private JLabel lblNewLabel;
	private JLabel lblNewLabel_1;
	private JLabel lblNewLabel_2;
	private JTextField frameworkLogLevelTextField;
	private JTextField frameworkLogTypeTextField;
	private JTextField frameworkStdLogLevelTextField;
	private final JLabel lblNewLabel_3 = new JLabel("storage (class_name):");
	private JTextField storageClassNameTextField;
	private JLabel lblStorageProperties;
	private JLabel lblProtocolproperties;
	private JPanel panel_1;
	private JButton btnClearn;
	private JLabel label_3;

	public String id;
	private KeyValueParameters protocolProperties;
	private KeyValueParameters storageProperties;
	private JPanel panel_2;
	private JPanel panel_3;
	private JButton btnSetProperties;
	private JButton btnSetProperties_1;
	private JLabel lblNewLabel_4;
	private JTextField workloadTextField;
	private JLabel lblYcsbdriver;
	private JTextField ycsbDriverTextField;
	private JPanel panel;
	private JTextField idTextField;
	private JLabel lblId;

	Component caller;
	boolean isDefaults;
	private JLabel lblIp;
	public JTextField ipTextField;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					EditExperimentParametersFrame frame = new EditExperimentParametersFrame("Test", false, null);
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 * 
	 * @wbp.parser.constructor
	 */
	public EditExperimentParametersFrame(String id, boolean isDefaults, Component caller) {
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosed(WindowEvent e) {
				if (caller != null) {
					caller.updateId(idTextField.getText());
				}
			}
		});
		this.id = id;
		this.caller = caller;
		this.isDefaults = isDefaults;

		protocolProperties = new KeyValueParameters();
		protocolProperties.setTitle("protocol_properties - " + id);
		protocolProperties.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		storageProperties = new KeyValueParameters();
		storageProperties.setTitle("Storage properties - " + id);
		storageProperties.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		setTitle("Edit Parameters");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 613, 661);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		setContentPane(contentPane);
		contentPane.setLayout(new GridLayout(0, 2, 0, 0));

		if (!isDefaults) {
			lblId = new JLabel("id:");
			lblId.setHorizontalAlignment(SwingConstants.RIGHT);
			contentPane.add(lblId);

			idTextField = new JTextField();
			contentPane.add(idTextField);
			idTextField.setColumns(10);
			idTextField.setText(id);

			lblIp = new JLabel("ip:");
			lblIp.setHorizontalAlignment(SwingConstants.RIGHT);
			contentPane.add(lblIp);

			ipTextField = new JTextField();
			contentPane.add(ipTextField);
			ipTextField.setColumns(10);
		}
		JLabel lblName = new JLabel("client_jar_file:");
		lblName.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblName);

		clientJarFileTextField = new JTextField();
		clientJarFileTextField.setColumns(10);
		contentPane.add(clientJarFileTextField);

		JLabel lblClientclassname = new JLabel("client_class_name:");
		lblClientclassname.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblClientclassname);

		clientClassNameTextField = new JTextField();
		clientClassNameTextField.setColumns(10);
		contentPane.add(clientClassNameTextField);

		lblUsername = new JLabel("username:");
		lblUsername.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblUsername);

		usernameTextField = new JTextField();
		usernameTextField.setColumns(10);
		contentPane.add(usernameTextField);

		lblPassword = new JLabel("password:");
		lblPassword.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblPassword);

		passwordTextField = new JTextField();
		passwordTextField.setColumns(10);
		contentPane.add(passwordTextField);

		lblKey = new JLabel("key:");
		lblKey.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblKey);

		keyTextField = new JTextField();
		keyTextField.setColumns(10);
		contentPane.add(keyTextField);

		lblNewLabel_4 = new JLabel("workload:");
		lblNewLabel_4.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblNewLabel_4);

		workloadTextField = new JTextField();
		contentPane.add(workloadTextField);
		workloadTextField.setColumns(10);

		lblYcsbdriver = new JLabel("ycsb_driver:");
		lblYcsbdriver.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblYcsbdriver);

		ycsbDriverTextField = new JTextField();
		contentPane.add(ycsbDriverTextField);
		ycsbDriverTextField.setColumns(10);

		lblWorkingdirectory = new JLabel("working_directory:");
		lblWorkingdirectory.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblWorkingdirectory);

		workingDirectoryTextField = new JTextField();
		workingDirectoryTextField.setColumns(10);
		contentPane.add(workingDirectoryTextField);

		lblConnectorsleeptime = new JLabel("connector_sleep_time:");
		lblConnectorsleeptime.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblConnectorsleeptime);

		connectorSleepTImeTextField = new JTextField();
		connectorSleepTImeTextField.setColumns(10);
		contentPane.add(connectorSleepTImeTextField);

		lblChannelcapacity = new JLabel("os:");
		lblChannelcapacity.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblChannelcapacity);

		osTextField = new JTextField();
		osTextField.setColumns(10);
		contentPane.add(osTextField);

		lblProtocollogfile = new JLabel("protocol_log_file:");
		lblProtocollogfile.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblProtocollogfile);

		protocolLogFileTextField = new JTextField();
		protocolLogFileTextField.setColumns(10);
		contentPane.add(protocolLogFileTextField);

		lblProtocolloglevel = new JLabel("protocol_log_level:");
		lblProtocolloglevel.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblProtocolloglevel);

		protocolLogLevelTextField = new JTextField();
		protocolLogLevelTextField.setColumns(10);
		contentPane.add(protocolLogLevelTextField);

		lblProtocollogtype = new JLabel("protocol_log_type:");
		lblProtocollogtype.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblProtocollogtype);

		protocolLogTypeTextField = new JTextField();
		protocolLogTypeTextField.setColumns(10);
		contentPane.add(protocolLogTypeTextField);

		lblProtocolstdloglevel = new JLabel("protocol_std_log_level:");
		lblProtocolstdloglevel.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblProtocolstdloglevel);

		protocolStdLogLevelTextField = new JTextField();
		protocolStdLogLevelTextField.setColumns(10);
		contentPane.add(protocolStdLogLevelTextField);

		lblFrameworklogfile = new JLabel("framework_log_file:");
		lblFrameworklogfile.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblFrameworklogfile);

		frameworkLogFileTextField = new JTextField();
		frameworkLogFileTextField.setColumns(10);
		contentPane.add(frameworkLogFileTextField);

		lblNewLabel = new JLabel("framework_log_level:");
		lblNewLabel.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblNewLabel);

		frameworkLogLevelTextField = new JTextField();
		contentPane.add(frameworkLogLevelTextField);
		frameworkLogLevelTextField.setColumns(10);

		lblNewLabel_1 = new JLabel("framework_log_type:");
		lblNewLabel_1.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblNewLabel_1);

		frameworkLogTypeTextField = new JTextField();
		contentPane.add(frameworkLogTypeTextField);
		frameworkLogTypeTextField.setColumns(10);

		lblNewLabel_2 = new JLabel("framework_std_log_level:");
		lblNewLabel_2.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblNewLabel_2);

		frameworkStdLogLevelTextField = new JTextField();
		contentPane.add(frameworkStdLogLevelTextField);
		frameworkStdLogLevelTextField.setColumns(10);
		lblNewLabel_3.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblNewLabel_3);

		storageClassNameTextField = new JTextField();
		contentPane.add(storageClassNameTextField);
		storageClassNameTextField.setColumns(10);

		lblStorageProperties = new JLabel("Storage Properties:");
		lblStorageProperties.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblStorageProperties);

		panel_2 = new JPanel();
		contentPane.add(panel_2);
		panel_2.setLayout(new BoxLayout(panel_2, BoxLayout.X_AXIS));

		btnSetProperties = new JButton("Set Properties");
		btnSetProperties.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				storageProperties.setVisible(true);
			}
		});
		panel_2.add(btnSetProperties);

		lblProtocolproperties = new JLabel("protocol_properties:");
		lblProtocolproperties.setHorizontalAlignment(SwingConstants.RIGHT);
		contentPane.add(lblProtocolproperties);

		panel_3 = new JPanel();
		contentPane.add(panel_3);
		panel_3.setLayout(new BoxLayout(panel_3, BoxLayout.X_AXIS));

		btnSetProperties_1 = new JButton("Set Properties");
		btnSetProperties_1.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				protocolProperties.setVisible(true);
			}
		});
		panel_3.add(btnSetProperties_1);

		panel = new JPanel();
		contentPane.add(panel);

		panel_1 = new JPanel();
		contentPane.add(panel_1);
		panel_1.setLayout(new BoxLayout(panel_1, BoxLayout.X_AXIS));
		
				btnClearn = new JButton("Clear");
				btnClearn.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						for (java.awt.Component c : contentPane.getComponents()) {
							if (c.getClass().equals(JTextField.class)) {
								((JTextField) c).setText("");
							}
						}
					}
				});
				btnClearn.setVerticalAlignment(SwingConstants.TOP);
				panel_1.add(btnClearn);

		label_3 = new JLabel("");
		panel_1.add(label_3);
	}

	public EditExperimentParametersFrame(String id, EditExperimentParametersFrame that, ClientComponent thatCaller) {
		this(id, that.isDefaults, thatCaller);
		for (int i = 0; i < that.contentPane.getComponentCount(); i++) {

			java.awt.Component c = that.contentPane.getComponent(i);
			System.out.println(c.getClass());
			if (c.getClass().equals(JTextField.class)) {
				((JTextField) this.contentPane.getComponent(i)).setText(((JTextField) c).getText());
				System.out.println("TestFiled: " + ((JTextField) c).getText());
			}
		}
		idTextField.setText(id);

		protocolProperties = new KeyValueParameters(that.protocolProperties);
		protocolProperties.setTitle("protocol_properties - " + id);
		protocolProperties.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		storageProperties = new KeyValueParameters(that.storageProperties);
		storageProperties.setTitle("Storage properties - " + id);
		storageProperties.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
	}

	public Parameters getParameters() {
		Parameters result = new Parameters();
		if (!clientClassNameTextField.getText().isEmpty())
			result.setClientClassName(clientClassNameTextField.getText());
		if (!clientJarFileTextField.getText().isEmpty())
			result.setClientJarFile(clientJarFileTextField.getText());
		if (!connectorSleepTImeTextField.getText().isEmpty())
			result.setConnectorSleepTime(connectorSleepTImeTextField.getText());
		if (!frameworkLogFileTextField.getText().isEmpty())
			result.setFrameworkLogFile(frameworkLogFileTextField.getText());
		if (!frameworkLogLevelTextField.getText().isEmpty())
			result.setFrameworkLogLevel(frameworkLogLevelTextField.getText());
		if (!frameworkLogTypeTextField.getText().isEmpty())
			result.setFrameworkLogType(frameworkLogTypeTextField.getText());
		if(!frameworkStdLogLevelTextField.getText().isEmpty())
			result.setFrameworkStdLogLevel(frameworkStdLogLevelTextField.getText());
		if (!keyTextField.getText().isEmpty())
			result.setKey(keyTextField.getText());
		if(!osTextField.getText().isEmpty())
				result.setOs(osTextField.getText());
		if (!passwordTextField.getText().isEmpty())
			result.setPassword(passwordTextField.getText());
		if(!protocolLogFileTextField.getText().isEmpty())
			result.setProtocolLogFile(protocolLogFileTextField.getText());
		if(!protocolLogLevelTextField.getText().isEmpty())
			result.setProtocolLogLevel(protocolLogLevelTextField.getText());
		if(!protocolLogTypeTextField.getText().isEmpty())
			result.setProtocolLogType(protocolLogTypeTextField.getText());
		if(!protocolStdLogLevelTextField.getText().isEmpty())
			result.setProtocolStdLogLevel(protocolStdLogLevelTextField.getText());
		if(!usernameTextField.getText().isEmpty())
			result.setUsername(usernameTextField.getText());
		if(!workingDirectoryTextField.getText().isEmpty())
			result.setWorkingDirectory(workingDirectoryTextField.getText());
		if(!ycsbDriverTextField.getText().isEmpty())
			result.setYcsbDriver(ycsbDriverTextField.getText());
		if(!workloadTextField.getText().isEmpty())
			result.setWorkload(workloadTextField.getText());
		
		List<Property> sProperties = storageProperties.getProperties();
		//debug
		System.out.println("Storage properties size: " + sProperties.size());
		if (!storageClassNameTextField.getText().isEmpty() || !sProperties.isEmpty()) {
			Storage sc = new Storage();
			if (!storageClassNameTextField.getText().isEmpty())
				sc.setClassName(storageClassNameTextField.getText());
			
			List<edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property> propertiesExp = new ArrayList<>();
			for (Property p : sProperties){
				edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property newProperty = new edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property();
				newProperty.setKey(p.getKey());
				newProperty.setValue(p.getValue());
				propertiesExp.add(newProperty);
			}
			sc.getProperty().addAll(propertiesExp);
			result.setStorage(sc);
		}

		ProtocolProperties pp = new ProtocolProperties();
		List<Property> ppGiven = protocolProperties.getProperties();
		List<edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property> propertiesExp = new ArrayList<>();
		for (Property p : ppGiven){
			edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property newProperty = new edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property();
			newProperty.setKey(p.getKey());
			newProperty.setValue(p.getValue());
			propertiesExp.add(newProperty);
		}
		if (!ppGiven.isEmpty()) {
			pp.getProperty().addAll(propertiesExp);
			result.setProtocolProperties(pp);
		}
		
		return result;
	}

	public void save(ObjectOutputStream out) throws IOException {
		if (!isDefaults) {
			out.writeObject(idTextField.getText());
			out.writeObject(ipTextField.getText());
		}
		out.writeObject(clientClassNameTextField.getText());
		out.writeObject(clientJarFileTextField.getText());
		out.writeObject(usernameTextField.getText());
		out.writeObject(passwordTextField.getText());
		out.writeObject(keyTextField.getText());
		out.writeObject(workingDirectoryTextField.getText());
		out.writeObject(workloadTextField.getText());
		out.writeObject(ycsbDriverTextField.getText());
		out.writeObject(connectorSleepTImeTextField.getText());
		out.writeObject(osTextField.getText());
		out.writeObject(protocolLogFileTextField.getText());
		out.writeObject(protocolLogLevelTextField.getText());
		out.writeObject(protocolLogTypeTextField.getText());
		out.writeObject(protocolStdLogLevelTextField.getText());
		out.writeObject(frameworkLogFileTextField.getText());
		out.writeObject(frameworkLogLevelTextField.getText());
		out.writeObject(frameworkLogTypeTextField.getText());
		out.writeObject(frameworkStdLogLevelTextField.getText());
		out.writeObject(storageClassNameTextField.getText());
		protocolProperties.save(out);
		storageProperties.save(out);
	}
	
	public static EditExperimentParametersFrame load (ObjectInputStream input, String id, boolean isDefaults, Component caller) throws ClassNotFoundException, IOException{
		EditExperimentParametersFrame result = new EditExperimentParametersFrame(id, isDefaults, caller);
		if (!isDefaults){
			result.idTextField.setText((String)input.readObject());
			result.ipTextField.setText((String) input.readObject());
		}
		
		result.clientClassNameTextField.setText((String) input.readObject());
		result.clientJarFileTextField.setText((String) input.readObject());
		result.usernameTextField.setText((String) input.readObject());
		result.passwordTextField.setText((String) input.readObject());
		result.keyTextField.setText((String) input.readObject());
		result.workingDirectoryTextField.setText((String) input.readObject());
		result.workloadTextField.setText((String) input.readObject());
		result.ycsbDriverTextField.setText((String) input.readObject());
		result.connectorSleepTImeTextField.setText((String) input.readObject());
		result.osTextField.setText((String) input.readObject());
		result.protocolLogFileTextField.setText((String) input.readObject());
		result.protocolLogLevelTextField.setText((String) input.readObject());
		result.protocolLogTypeTextField.setText((String) input.readObject());
		result.protocolStdLogLevelTextField.setText((String) input.readObject());
		result.frameworkLogFileTextField.setText((String) input.readObject());
		result.frameworkLogLevelTextField.setText((String) input.readObject());
		result.frameworkLogTypeTextField.setText((String) input.readObject());
		result.frameworkStdLogLevelTextField.setText((String) input.readObject());
		result.storageClassNameTextField.setText((String) input.readObject());
		result.protocolProperties = KeyValueParameters.load(input);
		result.storageProperties = KeyValueParameters.load(input);
		
		result.protocolProperties.setTitle("protocol_properties - " + id);
		result.storageProperties.setTitle("Storage properties - " + id);
		return result;
	}
}
