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

import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Property;
import edu.msu.cse.dkvf.clusterManager.experimentDescriptor.Variation;

import javax.swing.JTable;
import javax.swing.JButton;
import javax.swing.JScrollPane;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import javax.swing.JTextField;
import javax.swing.JLabel;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class EditVariationFrame extends JFrame {

	JPanel contentPane;
	JTable myTable;
	 
	int numOfCopies = 0;
	
	String variationName;
	EditWorkloadVariations workloadWindow;
	private JTextField nameTextField;

	/**
	 * Create the frame.
	 */
	public EditVariationFrame(String name, EditWorkloadVariations workloadWindow) {
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosing(WindowEvent e) {
				String newName  = nameTextField.getText();
				workloadWindow.updateName(variationName, newName);
				variationName = newName;
				if (myTable.getCellEditor() != null)
					myTable.getCellEditor().stopCellEditing();
			}
		});
		this.variationName = name;
		this.workloadWindow = workloadWindow;
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
		
		JButton btnNewButton_2 = new JButton("Duplicate");
		btnNewButton_2.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				duplicate();
			}
		});

		
		JLabel lblName = new JLabel("Name:");
		panel.add(lblName);
		
		nameTextField = new JTextField();
		panel.add(nameTextField);
		nameTextField.setColumns(10);
		nameTextField.setText(variationName);
		
		JButton btnDelete = new JButton("Delete");
		btnDelete.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String newName  = nameTextField.getText();
				workloadWindow.updateName(variationName, newName);
				variationName = newName;
				workloadWindow.removeVariation(nameTextField.getText());
				EditVariationFrame.this.dispose();
			}
		});
		panel.add(btnDelete);
		panel.add(btnNewButton_1);
		panel.add(btnNewButton_2);
		
	}
	
	public EditVariationFrame(String name, EditVariationFrame that) {
		this(name, that.workloadWindow);
		for (int r = 0 ; r < that.myTable.getRowCount(); r++){
			myTable.setValueAt(that.myTable.getValueAt(r, 0), r, 0);
			myTable.setValueAt(that.myTable.getValueAt(r, 1), r, 1);
		}
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
				Property newProperty = new Property();
				newProperty.setKey(key);
				newProperty.setValue(value);
				result.add(newProperty);
			}
		}
		return result;
	}


	public Variation getVariation() {
		Variation variation = new Variation();
		variation.setName(nameTextField.getText());
		variation.getProperty().addAll(getProperties());
		return variation;
	}


	public void save(ObjectOutputStream output) throws IOException {
		variationName = nameTextField.getText();
		output.writeObject(variationName);
		for (int r = 0 ; r < myTable.getRowCount(); r++){
			output.writeObject(myTable.getValueAt(r, 0));
			output.writeObject(myTable.getValueAt(r, 1));
		}
	}
	
	public static EditVariationFrame load(ObjectInputStream input, EditWorkloadVariations workloadWindow) throws ClassNotFoundException, IOException {
		String name = (String)input.readObject();
		EditVariationFrame kvp = new EditVariationFrame(name, workloadWindow);
		for (int r = 0 ; r < kvp.myTable.getRowCount(); r++){
			kvp.myTable.setValueAt(input.readObject(), r, 0);
			kvp.myTable.setValueAt(input.readObject(), r, 1);
		}
		kvp.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		return kvp;
	}
	
	
	void duplicate (){
		String newName  = nameTextField.getText();
		workloadWindow.updateName(variationName, newName);
		variationName = newName;
		if (myTable.getCellEditor() != null)
			myTable.getCellEditor().stopCellEditing();
		String name = nameTextField.getText()+"_copy_" + numOfCopies++;
		JButton btnNewButton = new JButton(name);
		EditVariationFrame evf = new EditVariationFrame(name, this);
		evf.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		workloadWindow.variationFrames.put(name, evf);
		workloadWindow.variationButtons.put(name, btnNewButton);
		btnNewButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				evf.setVisible(true);
			}
		});
		
		workloadWindow.variationPanel.add(btnNewButton);
		workloadWindow.validate();
	}
}
