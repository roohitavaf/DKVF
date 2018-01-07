package edu.msu.cse.dkvf;

  
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.logging.Logger;

import edu.msu.cse.dkvf.metadata.Metadata.Record;

/**
 * The storage layer. Any driver written for a specific product must extend this 
 * class to let the framework work with it. 
 * 
 * 
 */
public abstract class Storage {
	
	/**
	 * 
	 * The status of a storage operation.
	 *
	 */
	public enum StorageStatus {
		SUCCESS, FAILURE
	}
	
	/**
	 * Initializes the storage engine. 
	 * @param parameters
	 * 			A map of properties required by the storage engine to initialized. 
	 * @param logger
	 * 			The logger used by the storage engine. 
	 * @return
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus init(HashMap<String, String> parameters, Logger logger);
	
	
	/**
	 * Inserts/updates a value for the given key.  
	 * @param key	
	 * 			The record key of the key to insert. 
	 * @param value
	 * 			The value to insert to the record. 
	 * @return 
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus insert(String key, Record value);
	
	/**
	 * Reads the value of a key. 
	 * 
	 * @param key
	 * 			The record key of the key to read. 
	 * @param p
	 * 			The predicate that must be true for the value to be returned. 
	 * @param result
	 * 			The value of the record. . Only the first element of the list will be used as the result value.
	 * 			If the list is empty, key does not exists.  
	 * @return
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus read(String key, Predicate<Record> p, List<Record> result);
	
	/**
	 * Runs the storage engine. 
	 * @return
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus run();
	
	/**
	 * Closes the storage engine. 
	 * @return
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus close ();
	/**
	 * Cleans the whole data. 
	 * @return
	 * 			The result of the operation. 
	 */
	public abstract StorageStatus clean();
	

}
