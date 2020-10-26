package edu.msu.cse.dkvf.clusterDesigner;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;
import javax.swing.table.DefaultTableModel;

import edu.msu.cse.dkvf.config.Property;
import edu.msu.cse.dkvf.config.Storage;

import javax.swing.JTable;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JLabel;
import javax.swing.JTextField;


public class StorageParameters extends JFrame {

	private JPanel contentPane;
	private int nProperties;
	private JTextField textField;
	private JTable myTable;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					StorageParameters frame = new StorageParameters(100);
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public StorageParameters(int nProperties) {
		setTitle("Storage Parameter");
		this.nProperties = nProperties;
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 387);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		JPanel panel = new JPanel();
		contentPane.add(panel, BorderLayout.SOUTH);
		
		
		JButton btnNewButton = new JButton("OK");
		panel.add(btnNewButton);
		
		JButton btnNewButton_1 = new JButton("Clear");
		btnNewButton_1.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				clear();
			}
		});
		panel.add(btnNewButton_1);
		
		JPanel panel_1 = new JPanel();
		contentPane.add(panel_1, BorderLayout.NORTH);
		
		JLabel lblClassname = new JLabel("class_name:");
		panel_1.add(lblClassname);
		
		textField = new JTextField();
		panel_1.add(textField);
		textField.setColumns(30);
		
		myTable = new JTable(new DefaultTableModel(new Object[]{"Parameter", "Value"}, nProperties));
		JScrollPane scrollPane = new JScrollPane(myTable, JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED, JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
		
		contentPane.add(scrollPane, BorderLayout.CENTER);
	}
	
	public Map<String, String> getMap (){
		Map<String, String> result = new HashMap<String, String>();
		 for (int i = 0 ; i < nProperties ; i++){
			 String key = (String)myTable.getValueAt(i, 0);
			 if (!key.isEmpty()){
				 String value = (String)myTable.getValueAt(i, 1);
				 result.put(key, value);
			 }
		 }
		return result;
	}
	
	public Storage getStorage(){
		Storage s = new Storage();
		s.setClassName(textField.getText());
		for (int i = 0 ; i < nProperties ; i++){
			 String key = (String)myTable.getValueAt(i, 0);
			 if (!key.isEmpty()){
				 String value = (String)myTable.getValueAt(i, 1);
				 Property p = new Property();
				 p.setKey(key);
				 p.setValue(value);
				 s.getProperty().add(p);
			 }
		 }
		return s;
	}
	
	private void clear(){
		textField.setText("");
		for (int i = 0; i < myTable.getRowCount(); i++) {
	        for (int j = 0; j < myTable.getColumnCount(); j++) {
	        	myTable.setValueAt("", i, j);
	        }
	    }
	}
	

}
