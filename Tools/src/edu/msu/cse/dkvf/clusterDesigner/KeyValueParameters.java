package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;

import edu.msu.cse.dkvf.clusterManager.clusterDescriptor.Property;

import javax.swing.JTable;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.event.ActionListener;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.awt.event.ActionEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;


public class KeyValueParameters extends JFrame {

	private JPanel contentPane;
	private JTable myTable;

	
	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					KeyValueParameters frame = new KeyValueParameters();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	
	/**
	 * @wbp.parser.constructor
	 */
	public KeyValueParameters (KeyValueParameters that){
	
		this();

		clear();
		for (int r = 0 ; r < that.myTable.getRowCount(); r++){
			myTable.setValueAt(that.myTable.getValueAt(r, 0),r, 0);
			myTable.setValueAt(that.myTable.getValueAt(r, 1),r, 1);
		}
	}
	/**
	 * Create the frame.
	 */
	public KeyValueParameters() {
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 387);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		myTable = new JTable(new DefaultTableModel(new Object[]{"Parameter", "Value"}, ClusterDesignerApplication.NUM_OF_PROPERTIES));
		JScrollPane scrollPane = new JScrollPane(myTable, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		
		contentPane.add(scrollPane, BorderLayout.CENTER);
		
		JPanel panel = new JPanel();
		contentPane.add(panel, BorderLayout.SOUTH);
		
		JButton btnNewButton_1 = new JButton("Clear");
		btnNewButton_1.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				clear();
			}
		});
		panel.add(btnNewButton_1);
		
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				if (myTable.getCellEditor() != null)
					myTable.getCellEditor().stopCellEditing();
			}
		});
	}
	
	public Map<String, String> getMap (){
		Map<String, String> result = new HashMap<String, String>();
		 for (int i = 0 ; i < ClusterDesignerApplication.NUM_OF_PROPERTIES ; i++){
			 String key = (String)myTable.getValueAt(i, 0);
			 if (!key.isEmpty()){
				 String value = (String)myTable.getValueAt(i, 1);
				 result.put(key, value);
			 }
		 }
		return result;
	}
	
	private void clear(){
		System.out.println("clear");
		for (int i = 0; i < myTable.getRowCount(); i++) {
	        for (int j = 0; j < myTable.getColumnCount(); j++) {
	        	myTable.setValueAt("", i, j);
	        }
	    }
	}
	
	public List<Property> getProperties (){
		List<Property> result = new ArrayList<Property>();
		for (int r = 0 ; r < myTable.getRowCount(); r++){
			String key = (String)myTable.getValueAt(r, 0);
			String value = (String)myTable.getValueAt(r, 1);
			if (key != null && !key.isEmpty()){
				//debug
				System.out.println("key= " + key);
				Property newProperty = new Property();
				newProperty.setKey(key);
				newProperty.setValue(value);
				result.add(newProperty);
			}
		}
		return result;
	}


	public void save(ObjectOutputStream out) throws IOException {
		for (int r = 0 ; r < myTable.getRowCount(); r++){
			out.writeObject(myTable.getValueAt(r, 0));
			out.writeObject(myTable.getValueAt(r, 1));
		}
		
	}


	public static KeyValueParameters load(ObjectInputStream input) throws ClassNotFoundException, IOException {
		KeyValueParameters kvp = new KeyValueParameters();
		for (int r = 0 ; r < kvp.myTable.getRowCount(); r++){
			kvp.myTable.setValueAt(input.readObject(), r, 0);
			kvp.myTable.setValueAt(input.readObject(), r, 1);
		}
		kvp.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		return kvp;
	}
}
