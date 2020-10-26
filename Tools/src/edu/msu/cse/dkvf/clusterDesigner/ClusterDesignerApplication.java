package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.EventQueue;
import java.awt.GridLayout;

import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.xml.bind.JAXBException;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Cluster;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.ClusterReaderWriter;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Connect;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Parameters;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Server;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Servers;
import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Topology;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Client;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Clients;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ConnectTo;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Experiment;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ExperimentWriter;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.ServerInfo;

import java.awt.BorderLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;

import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.swing.JTextField;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JToggleButton;
import javax.swing.JCheckBox;

public class ClusterDesignerApplication {

	private JFrame frmClusterDesigner;
	final static int NUM_OF_PROPERTIES = 100;
	JFileChooser fc = new JFileChooser();

	static ClusterDesignerPanel panel;
	static Map<String, HubComponent> hubs = new HashMap();
	static Map<String, ServerComponent> servers = new HashMap();
	static Map<String, ClientComponent> clients = new HashMap();

	static boolean isConnecting = false;
	static Component point1 = null;
	static boolean isDisconnecting = false;
	static Component selectedComponent = null;
	int numOfServers = 0;
	int numOfClients = 0;
	static EditServerParametersFrame defaultClusterParameters;
	EditExperimentParametersFrame defaultExperimentParameters;
	EditWorkloadVariations workloadVariations;
	private JTextField clusterNameTextField;
	private JTextField experimentNameTextField;

	File currentFile = null;
	
	FileNameExtensionFilter filter = new FileNameExtensionFilter("DKFV Cluster Desinger (*.dcd)", "dcd");

	static HashMap<String, String> ipTable = new HashMap<>();
	static String ipTableFileAddress = null;
	static JFileChooser ipFc = new JFileChooser();
	static JCheckBox ipMappingChk = new JCheckBox("Use IP Mapping");

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					ClusterDesignerApplication window = new ClusterDesignerApplication();
					window.frmClusterDesigner.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the application.
	 */
	public ClusterDesignerApplication() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		frmClusterDesigner = new JFrame();
		frmClusterDesigner.setTitle("Cluster Designer");
		frmClusterDesigner.setBounds(100, 100, 771, 400);
		frmClusterDesigner.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		defaultClusterParameters = new EditServerParametersFrame("Defaults", true, null);
		defaultClusterParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		defaultExperimentParameters = new EditExperimentParametersFrame("Defaults", true, null);
		defaultExperimentParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		workloadVariations = new EditWorkloadVariations();
		workloadVariations.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		
		
		ipFc.setDialogTitle("IP Table File");
		JMenuBar menuBar = new JMenuBar();
		menuBar.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}

			@Override
			public void mousePressed(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});
		frmClusterDesigner.setJMenuBar(menuBar);

		JMenu mnFile = new JMenu("File");
		mnFile.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});
		menuBar.add(mnFile);

		JMenuItem mntmNew = new JMenuItem("New");
		mntmNew.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int dialogButton = JOptionPane.YES_NO_OPTION;
				int dialogResult = JOptionPane.showConfirmDialog(null, "Are you sure to delete current desing?", "Warning", dialogButton);
				if (dialogResult == JOptionPane.YES_OPTION) {
					clear();
				}

			}
		});
		mnFile.add(mntmNew);

		mnFile.addSeparator();

		JMenuItem mntmOpenFile = new JMenuItem("Open File...");
		mntmOpenFile.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				fc.setDialogTitle("Open");
				fc.setApproveButtonText("Open");
				fc.addChoosableFileFilter(filter);

				int returnVal = fc.showOpenDialog(panel);

				if (returnVal == JFileChooser.APPROVE_OPTION) {
					clear();
					open(fc.getSelectedFile());

				}
			}
		});
		mnFile.add(mntmOpenFile);

		mntmOpenFile.setAccelerator(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_O, java.awt.Event.CTRL_MASK));

		mnFile.addSeparator();

		JMenuItem mntmSave = new JMenuItem("Save");
		mntmSave.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				if (currentFile != null)
					save(currentFile);
				else
					saveAs();
			}
		});
		mnFile.add(mntmSave);
		mntmSave.setAccelerator(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_S, java.awt.Event.CTRL_MASK));

		JMenuItem mntmSaveAs = new JMenuItem("Save As...");
		mntmSaveAs.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				saveAs();
			}
		});
		mnFile.add(mntmSaveAs);

		mnFile.addSeparator();

		JMenuItem mntmExpoertCluster = new JMenuItem("Export Cluster");
		mntmExpoertCluster.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				fc.setApproveButtonText("Export");
				fc.setDialogTitle("Export Cluster");
				fc.setSelectedFile(new File(clusterNameTextField.getText() + ".xml"));
				fc.removeChoosableFileFilter(filter);
				int returnVal = fc.showDialog(panel, "Export");

				if (returnVal == JFileChooser.APPROVE_OPTION) {
					try {
						ClusterReaderWriter.writeCluster(getCluster(), new FileOutputStream(fc.getSelectedFile()));
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (JAXBException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			}
		});
		mnFile.add(mntmExpoertCluster);
		mntmExpoertCluster.setAccelerator(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_L, java.awt.Event.CTRL_MASK));

		JMenuItem mntmExportExperiment = new JMenuItem("Export Experiment");
		mntmExportExperiment.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				fc.setApproveButtonText("Export");
				fc.setDialogTitle("Export Experiment");
				fc.setSelectedFile(new File(experimentNameTextField.getText() + ".xml"));
				fc.removeChoosableFileFilter(filter);

				int returnVal = fc.showDialog(panel, "Export");

				if (returnVal == JFileChooser.APPROVE_OPTION) {
					try {
						ExperimentWriter.writeExperiment(getExperiment(), new FileOutputStream(fc.getSelectedFile(), false));
					} catch (FileNotFoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (JAXBException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}

			}
		});
		mnFile.add(mntmExportExperiment);

		mntmExportExperiment.setAccelerator(KeyStroke.getKeyStroke(java.awt.event.KeyEvent.VK_E, java.awt.Event.CTRL_MASK));

		mnFile.addSeparator();
		JMenuItem mntmExit = new JMenuItem("Exit");
		mnFile.add(mntmExit);

		JMenu mnCluster = new JMenu("Cluster");
		mnCluster.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});

		JMenu mnEdit_1 = new JMenu("Edit");
		menuBar.add(mnEdit_1);

		JMenuItem mntmCopy = new JMenuItem("Duplicate");
		mntmCopy.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				duplicateComponent();
			}
		});
		mnEdit_1.add(mntmCopy);

		JMenuItem mntmDelete = new JMenuItem("Delete");
		mntmDelete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				deleteComponent();
			}
		});
		mnEdit_1.add(mntmDelete);
		menuBar.add(mnCluster);

		JMenuItem mntmServer = new JMenuItem("Add Server");
		mntmServer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addServer();
			}
		});

		JMenuItem mntmEditParameters = new JMenuItem("Default Server Properties...");
		mntmEditParameters.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				EventQueue.invokeLater(new Runnable() {
					public void run() {
						try {
							defaultClusterParameters.setTitle("Default Cluster Properties");
							defaultClusterParameters.setVisible(true);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		});
		mnCluster.add(mntmEditParameters);

		mnCluster.addSeparator();
		mnCluster.add(mntmServer);

		JMenu mnHelp = new JMenu("Help");
		mnHelp.addMouseListener(new MouseAdapter() {
			@Override
			public void mousePressed(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});

		JMenu mnExperiment = new JMenu("Experiment");
		menuBar.add(mnExperiment);

		JMenuItem mntmDefaultClientProperties = new JMenuItem("Default Client Properties...");
		mntmDefaultClientProperties.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				EventQueue.invokeLater(new Runnable() {
					public void run() {
						try {
							defaultExperimentParameters.setTitle("Default Experiment Properties");
							defaultExperimentParameters.setVisible(true);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		});
		mnExperiment.add(mntmDefaultClientProperties);

		JMenuItem mntmEditWorkloadVariations = new JMenuItem("Workload Variations...");
		mntmEditWorkloadVariations.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				EventQueue.invokeLater(new Runnable() {
					public void run() {
						try {
							workloadVariations.setTitle("Workload Variations");
							workloadVariations.setVisible(true);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
		});
		mnExperiment.add(mntmEditWorkloadVariations);

		mnExperiment.addSeparator();
		JMenuItem mntmClient = new JMenuItem("Add Client");
		mntmClient.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addClient();
			}
		});
		mnExperiment.add(mntmClient);

		JMenu mnConnections = new JMenu("Connections");
		menuBar.add(mnConnections);
		menuBar.add(mnHelp);

		JMenuItem mntmAddHub = new JMenuItem("Add Hub");
		mntmAddHub.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addHub();
			}
		});
		mnConnections.add(mntmAddHub);
		mnConnections.addSeparator();
		JMenuItem mntmConnector = new JMenuItem("Connect");
		mntmConnector.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				connect();
			}
		});
		mnConnections.add(mntmConnector);

		JMenuItem mntmDisconnect = new JMenuItem("Disconnect");
		mntmDisconnect.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				disconnect();
			}
		});
		mnConnections.add(mntmDisconnect);
		mnConnections.addSeparator();

		JMenuItem mntmHelp = new JMenuItem("Help");
		mnHelp.add(mntmHelp);

		mnHelp.addSeparator();
		JMenuItem mntmAbout = new JMenuItem("About");
		mnHelp.add(mntmAbout);

		JPanel toolbarPan = new JPanel(new GridLayout(0, 1));
		frmClusterDesigner.getContentPane().add(toolbarPan, BorderLayout.NORTH);

		JToolBar buttonsToolBar = new JToolBar();
		buttonsToolBar.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});
		toolbarPan.add(buttonsToolBar, BorderLayout.NORTH);

		JToolBar textToolBar = new JToolBar();
		textToolBar.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseClicked(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});
		toolbarPan.add(textToolBar, BorderLayout.NORTH);

		JButton btnAddServer = new JButton("Add Server");
		btnAddServer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addServer();
			}
		});
		buttonsToolBar.add(btnAddServer);

		JButton btnAddClient = new JButton("Add Client");
		btnAddClient.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addClient();
			}
		});
		buttonsToolBar.add(btnAddClient);

		JButton btnAddConnector = new JButton("Connect");
		btnAddConnector.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				connect();
			}
		});

		JButton btnAddHub = new JButton("Add Hub");
		btnAddHub.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				addHub();
			}
		});
		buttonsToolBar.add(btnAddHub);
		buttonsToolBar.add(btnAddConnector);

		JButton btnDisconnect = new JButton("Disconnect");
		btnDisconnect.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				isConnecting = false;
				disconnect();
			}
		});
		buttonsToolBar.add(btnDisconnect);

		buttonsToolBar.addSeparator();

		JLabel lblClusterName = new JLabel("Cluster Name:");
		textToolBar.add(lblClusterName);

		clusterNameTextField = new JTextField();
		textToolBar.add(clusterNameTextField);
		clusterNameTextField.setColumns(9);

		JLabel lblExperimentName = new JLabel("Experiment name:");
		textToolBar.add(lblExperimentName);

		experimentNameTextField = new JTextField();
		textToolBar.add(experimentNameTextField);
		experimentNameTextField.setColumns(10);

		
		textToolBar.add(ipMappingChk);

		panel = new ClusterDesignerPanel();
		panel.addMouseListener(new MouseAdapter() {
			@Override
			public void mouseReleased(MouseEvent e) {
				isConnecting = false;
				isDisconnecting = false;
			}
		});
		frmClusterDesigner.getContentPane().add(panel, BorderLayout.CENTER);

		JMenuItem mntmSetIpTable = new JMenuItem("Set IP Table File...");
		mntmSetIpTable.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				int returnVal = ipFc.showOpenDialog(panel);

				if (returnVal == JFileChooser.APPROVE_OPTION) {
					ipTableFileAddress = ipFc.getSelectedFile().getAbsolutePath();
					readIpTable();
					ipMappingChk.setSelected(true);

				}
			}
		});
		mnConnections.add(mntmSetIpTable);

	}

	private void addHub() {
		isConnecting = false;
		isDisconnecting = false;
		System.out.println("Add HUB");
		HubComponent newHub = new HubComponent();
		hubs.put(newHub.id, newHub);
		panel.add(newHub, BorderLayout.CENTER);
		panel.repaint();
	}

	private void addServer() {
		isConnecting = false;
		isDisconnecting = false;
		System.out.println("Add server");
		String name = String.format("Server_%d", numOfServers++);
		ServerComponent newServer = new ServerComponent(name);
		panel.setLayout(null);
		panel.add(newServer, BorderLayout.CENTER);
		servers.put(name, newServer);
		panel.repaint();
	}

	private void duplicateComponent() {
		if (selectedComponent != null) {
			if (selectedComponent.getClass().equals(ServerComponent.class)) {
				ServerComponent newServer = new ServerComponent((ServerComponent) selectedComponent);
				servers.put(newServer.id, newServer);
				panel.add(newServer, BorderLayout.CENTER);
				panel.repaint();
			} else if (selectedComponent.getClass().equals(ClientComponent.class)) {
				ClientComponent newClient = new ClientComponent((ClientComponent) selectedComponent);
				clients.put(newClient.id, newClient);
				panel.add(newClient, BorderLayout.CENTER);
				panel.repaint();
			}
		}
	}

	private void deleteComponent() {
		if (selectedComponent != null) {
			panel.remove(selectedComponent);
			if (selectedComponent.getClass().equals(HubComponent.class) && hubs.containsKey(selectedComponent.id)) {
				hubs.remove(selectedComponent.id);
			}
			if (selectedComponent.getClass().equals(ServerComponent.class) && servers.containsKey(selectedComponent.id)) {
				System.out.println("Removed from servers");
				servers.remove(selectedComponent.id);
			}
			if (selectedComponent.getClass().equals(ClientComponent.class) && clients.containsKey(selectedComponent.id)) {
				clients.remove(selectedComponent.id);
			}
			HashMap<String, Connector> newConnector = new HashMap<>();
			for (Connector c : panel.connectors.values()) {
				if (selectedComponent != c.point1 && selectedComponent != c.point2) {
					newConnector.put(c.point1.id + "\0" + c.point2.id, c);
				}else {
					HubComponent hub = null;
					if (selectedComponent == c.point1 && c.point2.getClass().equals(HubComponent.class))
						hub = (HubComponent)c.point2;
					if (selectedComponent == c.point2 && c.point1.getClass().equals(HubComponent.class))
						hub = (HubComponent)c.point1;
					if (hub != null){
						hub.removePoint(selectedComponent);
					}
				}
			}
			panel.connectors = newConnector;
			if (point1 == selectedComponent)
				point1 = null;
			panel.repaint();
		}
	}

	private void addClient() {
		isConnecting = false;
		isDisconnecting = false;
		System.out.println("Add client");
		String name = String.format("Client_%d", numOfClients++);
		ClientComponent newClient = new ClientComponent(name);
		panel.setLayout(null);
		panel.add(newClient, BorderLayout.CENTER);
		clients.put(name, newClient);
		panel.repaint();
	}

	private void connect() {
		System.out.println("Connect");
		isConnecting = true;
		isDisconnecting = false;
		point1 = null;
	}

	private void disconnect() {
		System.out.println("disconnect");
		isDisconnecting = true;
		isConnecting = false;
		point1 = null;
	}

	public boolean isConnecting() {
		return isConnecting;
	}

	public boolean isDisconnecting() {
		return isDisconnecting;
	}

	public static Component getPoint1() {
		return point1;
	}

	public static void setPoint1(Component component) {
		System.out.println("setting point 1 " + component);
		point1 = component;
	}

	public static void setPoint2(Component component) {
		if (point1 == component)
			return;

		System.out.println("setting point 2" + component);
		if (isConnecting && !(panel.connectors.containsKey(point1.getId() + "\0" + component.getId()) || panel.connectors.containsKey(component.getId() + "\0" + point1.getId()))) {
			Connector newConnector = new Connector(point1, component);
			panel.connectors.put(point1.getId() + "\0" + component.getId(), newConnector);
			point1.connected(component);
			component.connected(point1);
			panel.repaint();
			//isConnecting = false;
			point1 = null;
		} else if (isDisconnecting) {
			panel.connectors.remove(point1.getId() + "\0" + component.getId());
			panel.connectors.remove(component.getId() + "\0" + point1.getId());
			point1.disconnect(component);
			component.disconnect(point1);
			panel.repaint();
			point1 = null;
		}
	}

	public void repaint() {
		frmClusterDesigner.repaint();
	}

	public Cluster getCluster() {
		if (ipMappingChk.isSelected()){
			readIpTable();
		}
		Cluster cluster = new Cluster();
		cluster.setName(clusterNameTextField.getText());
		cluster.setDefaults(defaultClusterParameters.getParameters());
		cluster.setServers(getServers());
		Topology topology = getTopology();
		if (!topology.getConnect().isEmpty())
			cluster.setTopology(topology);
		return cluster;
	}

	public Servers getServers() {
		Servers servers = new Servers();
		for (java.awt.Component c : panel.getComponents()) {
			if (c.getClass().equals(ServerComponent.class)) {
				ServerComponent sc = (ServerComponent) c;
				Server server = new Server();
				server.setId(sc.id);
				Parameters config = ((EditServerParametersFrame) sc.parameters).getParameters();
				if (config != null)
					server.setConfig(config);
				server.setIp(getActualIp(((EditServerParametersFrame) sc.parameters).ipTextField.getText()));
				servers.getServer().add(server);
			}
		}
		return servers;
	}

	public Topology getTopology() {
		Topology toplogy = new Topology();
		for (List<String> group : getGroups()) {
			Connect c = new Connect();
			c.getId().addAll(group);
			toplogy.getConnect().add(c);
		}
		return toplogy;
	}

	private List<List<String>> getGroups() {
		List<List<String>> result = new ArrayList<>();
		List<String> markedHubs = new ArrayList<>();
		for (HubComponent hub : hubs.values()) {
			if (!markedHubs.contains(hub)) {
				result.add(hub.getServersId(markedHubs));
			}
		}
		for (Connector c : panel.connectors.values()) {
			if (c.point1.getClass().equals(ServerComponent.class) && c.point2.getClass().equals(ServerComponent.class)) {
				List<String> pair = new ArrayList<>();
				pair.add(c.point1.id);
				pair.add(c.point2.id);
				result.add(pair);
			}
		}
		return result;
	}

	public Experiment getExperiment() {
		if (ipMappingChk.isSelected()){
			readIpTable();
		}
		Experiment result = new Experiment();
		result.setName(experimentNameTextField.getText());
		result.setDefaults(defaultExperimentParameters.getParameters());
		result.setClients(getClients());
		result.setWorkloadVariations(workloadVariations.getWorkloadVariations());
		return result;
	}

	Clients getClients() {
		Clients clients = new Clients();
		for (java.awt.Component c : panel.getComponents()) {
			if (c.getClass().equals(ClientComponent.class)) {
				ClientComponent cc = (ClientComponent) c;
				Client client = new Client();
				client.setId(cc.id);
				edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Parameters config = ((EditExperimentParametersFrame) cc.parameters).getParameters();
				if (config != null)
					client.setConfig(config);
				client.setIp(getActualIp(((EditExperimentParametersFrame) cc.parameters).ipTextField.getText()));
				client.setConnectTo(getConnectTo(cc));
				clients.getClient().add(client);
			}
		}
		return clients;
	}

	private ConnectTo getConnectTo(ClientComponent cc) {
		ConnectTo ct = new ConnectTo();
		List<String> markedHubs = new ArrayList<>();
		for (Connector c : panel.connectors.values()) {
			if (c.point1 == cc && c.point2.getClass().equals(ServerComponent.class)) {
				ServerComponent sc = (ServerComponent) c.point2;
				ServerInfo newServer = new ServerInfo();
				newServer.setId(sc.id);
				newServer.setIp(getActualIp(((EditServerParametersFrame) sc.parameters).ipTextField.getText()));
				String port = defaultClusterParameters.clientPortTextField.getText();
				if (((EditServerParametersFrame) sc.parameters).clientPortTextField.getText() != null && !((EditServerParametersFrame) sc.parameters).clientPortTextField.getText().isEmpty())
					port = ((EditServerParametersFrame) sc.parameters).clientPortTextField.getText();
				newServer.setPort(port);
				ct.getServer().add(newServer);
			} else if (c.point2 == cc && c.point1.getClass().equals(ServerComponent.class)) {
				ServerComponent sc = (ServerComponent) c.point1;
				ServerInfo newServer = new ServerInfo();
				newServer.setId(sc.id);
				newServer.setIp(getActualIp(((EditServerParametersFrame) sc.parameters).ipTextField.getText()));
				String port = defaultClusterParameters.clientPortTextField.getText();
				if (((EditServerParametersFrame) sc.parameters).clientPortTextField.getText() != null && !((EditServerParametersFrame) sc.parameters).clientPortTextField.getText().isEmpty())
					port = ((EditServerParametersFrame) sc.parameters).clientPortTextField.getText();
				newServer.setPort(port);
				ct.getServer().add(newServer);
			} else if (c.point1 == cc && c.point2.getClass().equals(HubComponent.class)) {
				HubComponent hub = (HubComponent) c.point2;
				ct.getServer().addAll(hub.getServersInfo(markedHubs));
			} else if (c.point2 == cc && c.point1.getClass().equals(HubComponent.class)) {
				HubComponent hub = (HubComponent) c.point1;
				ct.getServer().addAll(hub.getServersInfo(markedHubs));
			}
		}
		return ct;
	}

	void save(File file) {
		ObjectOutputStream output = null;
		try {
			output = new ObjectOutputStream(new FileOutputStream(file));
			output.writeObject(clusterNameTextField.getText());
			output.writeObject(experimentNameTextField.getText());
			defaultClusterParameters.save(output);
			defaultExperimentParameters.save(output);
			workloadVariations.save(output);

			//writing servers 
			output.writeInt(numOfServers);
			output.writeInt(servers.size());
			for (ServerComponent sc : servers.values()) {
				sc.save(output);
			}

			//writing clients 
			output.writeInt(numOfClients);
			output.writeInt(clients.size());
			for (ClientComponent cc : clients.values()) {
				cc.save(output);
			}

			//writing hubs
			output.writeInt(HubComponent.numberOfHubComponent);
			output.writeInt(hubs.size());
			for (HubComponent hc : hubs.values()) {
				hc.save(output);
			}

			//writing connectors
			output.writeInt(panel.connectors.size());
			for (Connector c : panel.connectors.values()) {
				c.save(output);
			}

			//writing ip table information
			output.writeBoolean(ipMappingChk.isSelected());
			if (ipMappingChk.isSelected())
				output.writeObject(ipTableFileAddress);
			
			output.close();
			currentFile = file;
			
			

		} catch (IOException x) {
			x.printStackTrace();
		} catch (NullPointerException n) {

		}
	}

	void open(File file) {
		ObjectInputStream input = null;
		try {
			input = new ObjectInputStream(new FileInputStream(file));
			clusterNameTextField.setText((String) input.readObject());
			experimentNameTextField.setText((String) input.readObject());
			defaultClusterParameters = EditServerParametersFrame.load(input, "Defaults", true, null);
			defaultClusterParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			defaultExperimentParameters = EditExperimentParametersFrame.load(input, "Defaults", true, null);
			defaultExperimentParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			workloadVariations = EditWorkloadVariations.load(input);
			workloadVariations.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

			servers = new HashMap<>();
			numOfServers = input.readInt();
			int nServers = input.readInt();
			for (int i = 0; i < nServers; i++) {
				ServerComponent newSc = ServerComponent.load(input);
				servers.put(newSc.id, newSc);
			}

			clients = new HashMap<>();
			numOfClients = input.readInt();
			int nClients = input.readInt();
			for (int i = 0; i < nClients; i++) {
				ClientComponent newCc = ClientComponent.load(input);
				clients.put(newCc.id, newCc);
			}

			hubs = new HashMap<>();
			HubComponent.numberOfHubComponent = input.readInt();
			int nHubs = input.readInt();
			for (int i = 0; i < nHubs; i++) {
				HubComponent newhc = HubComponent.load(input);
				hubs.put(newhc.id, newhc);
			}

			int nConnectors = input.readInt();
			for (int i = 0; i < nConnectors; i++) {
				Connector newC = Connector.load(input);
				panel.connectors.put(newC.point1.getId() + "\0" + newC.point2.getId(), newC);
			}
			panel.setLayout(null);
			ClusterDesignerApplication.panel.repaint();
			ClusterDesignerApplication.panel.revalidate();
			

			currentFile = file;
			frmClusterDesigner.setTitle(currentFile.getPath() + " - Cluster Designer");
			
			
			boolean ipMapping = input.readBoolean();
			if (ipMapping){
				ipMappingChk.setSelected(true);
				ipTableFileAddress= (String)input.readObject();
				readIpTable();
			}
			
			input.close();

		} catch (IOException x) {
			x.printStackTrace();
		} catch (ClassNotFoundException e) {

			e.printStackTrace();
		}
	}

	void clear() {
		frmClusterDesigner.remove(panel);
		frmClusterDesigner.setTitle("Cluster Designer");
		panel = new ClusterDesignerPanel();
		hubs.clear();
		isConnecting = false;
		point1 = null;
		isDisconnecting = false;
		selectedComponent = null;
		numOfServers = 0;
		numOfClients = 0;

		defaultClusterParameters = new EditServerParametersFrame("Defaults", true, null);
		defaultClusterParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		defaultExperimentParameters = new EditExperimentParametersFrame("Defaults", true, null);
		defaultExperimentParameters.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		workloadVariations = new EditWorkloadVariations();
		workloadVariations.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

		clusterNameTextField.setText("");
		experimentNameTextField.setText("");
		;
		currentFile = null;
		
		frmClusterDesigner.getContentPane().add(panel, BorderLayout.CENTER);
		frmClusterDesigner.revalidate();
		frmClusterDesigner.repaint();

		servers.clear();
		clients.clear();
		hubs.clear();
		
		ipTable.clear();
		ipTableFileAddress = null;
		ipFc = new JFileChooser();
		ipMappingChk.setSelected(false);
	}

	void saveAs() {
		fc.setDialogTitle("Save");
		fc.setApproveButtonText("Save");
		fc.setSelectedFile(currentFile);
		fc.addChoosableFileFilter(filter);
		int returnVal = fc.showSaveDialog(panel);

		if (returnVal == JFileChooser.APPROVE_OPTION) {
			save(fc.getSelectedFile());
			currentFile = fc.getSelectedFile();
			frmClusterDesigner.setTitle(currentFile.getPath() + " - Cluster Designer");
		}
	}

	void readIpTable() {
		if (ipTableFileAddress != null) {
			String data = "";
		    try {
				data = new String(Files.readAllBytes(Paths.get(ipTableFileAddress)));
				String[] lines = data.split("\n");
				for (String line : lines){
					String[] lineParts = line.split("\\s");
					ipTable.put(lineParts[0].trim(), lineParts[1].trim());
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		   
		}
	}
	static String getActualIp (String ip){
		if (ipMappingChk.isSelected()){
			System.out.println("Using IP table");
			if (ipTable.containsKey(ip))
				ip = ipTable.get(ip);
		}
		return ip;
	}
}
