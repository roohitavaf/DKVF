package edu.msu.cse.dkvf.bdbStorage;

import java.io.File;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import java.util.function.Predicate;
import java.util.logging.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.msu.cse.dkvf.Storage;
import edu.msu.cse.dkvf.Utils;
import edu.msu.cse.dkvf.metadata.Metadata.*;

/**
 * Driver for Berkeley-DB.
 *
 */
public class BDBStorage extends Storage {

	Logger logger;
	String directory;
	String name;
	boolean instantStable;
	boolean multiVersion;

	Database db;
	Environment env;

	String comparatorClassName;

	final static String DB_NAME_DEFAULT = "MyDB";
	final static String DB_DIRECTORY_DEFAULT = "DB";
	final static String INSTANT_STABLE_DEFAULT = "false";
	final static String MULTI_VERSION = "true";
	final static String COMPARATOR_CLASS_NAME_DEFAULT = "edu.msu.cse.dkvf.comparator.RecordCompartor";

	/**
	 * Initializes the storage engine. 
	 * @param storageConfig The configuration for the storage engine
	 * @param logger The logger
	 * @return The result of the operation
	 */
	public StorageStatus init(HashMap<String, String> storageConfig, Logger logger) {
		this.logger = logger;
		this.directory = setProperty(DB_DIRECTORY_DEFAULT, storageConfig.get("db_directory"));
		this.name = setProperty(DB_NAME_DEFAULT, storageConfig.get("db_name"));
		this.instantStable = Boolean.parseBoolean(setProperty(INSTANT_STABLE_DEFAULT, storageConfig.get("instant_stable")));
		this.multiVersion = Boolean.parseBoolean(setProperty(MULTI_VERSION, storageConfig.get("multi_version")));
		this.comparatorClassName = setProperty(COMPARATOR_CLASS_NAME_DEFAULT, storageConfig.get("comparator_class_name"));
		return StorageStatus.SUCCESS;
	}


	/**
	 * Inserts the given value for the given key
	 * @param key The key
	 * @param value The value
	 * @return The result of the operation
	 */
	public StorageStatus insert(String key, Record value) {
		try {
			DatabaseEntry myKey = new DatabaseEntry(key.getBytes());
			DatabaseEntry myData = new DatabaseEntry(value.toByteArray());
			if (db != null)
				db.put(null, myKey, myData);
			else {
				logger.severe("Problem in puting key= " + key + ". DB is not running properly.");
				return StorageStatus.FAILURE;
			}
			if (instantStable)
				db.sync();
			return StorageStatus.SUCCESS;
		} catch (Exception e) {
			logger.severe("Problem in puting key= " + key + " " + e.toString() + " Message: " + e.getMessage());
			return StorageStatus.FAILURE;
		}
	}

	/**
	 * Makes the data base information stable by writing them to the disk. 
	 * @return The result of the operation
	 */
	public StorageStatus makeStable() {
		try {
			if (db != null) {
				db.sync();
				return StorageStatus.SUCCESS;
			} else {
				logger.severe("Problem in making db stable: DB is not running properly.");
				return StorageStatus.FAILURE;
			}
		} catch (Exception e) {
			logger.severe("Problem in making db stable: " + e.toString());
			return StorageStatus.FAILURE;
		}
	}

	/**
	 * Cleans the entire data of the storage.
	 * @return The result of the operation
	 */
	public StorageStatus clean() {
		try {
			if (close() == StorageStatus.FAILURE)
				return StorageStatus.FAILURE;

			File DBfolder = new File(directory);
			for (File fileToDelete : DBfolder.listFiles())
				fileToDelete.delete();
			if (run() == StorageStatus.FAILURE)
				return StorageStatus.FAILURE;
			return StorageStatus.SUCCESS;
		} catch (Exception e) {
			logger.severe("Problem in cleaning db: " + e.toString() + "\n\tDirectory= " + directory);
			return StorageStatus.FAILURE;
		}
	}

	/**
	 * Closes the database. 
	 * @return The result of the operation
	 */
	public StorageStatus close() {
		try {
			if (db != null) {
				db.close();
			}

			if (env != null) {
				env.close();
			}
			return StorageStatus.SUCCESS;
		} catch (Exception e) {
			logger.severe("Problem in closing db: " + e.toString());
			return StorageStatus.FAILURE;
		}
	}
	
	/**
	 * Reads the first version of the data item with the given key that satisfies the given predicate.
	 * @param key The key of the data item to read. 
	 * @param p The predicate that need to be satisfied by the version. 
	 * @param result The list of containing the version that satisfies the given predicate. Note that although it is a list, only the first element should be used. 
	 * @return The result of the operation
	 */
	public StorageStatus read(String key, Predicate<Record> p, List<Record> result) {
		Cursor cursor = null;
		try {
			DatabaseEntry myKey = new DatabaseEntry(key.getBytes());
			DatabaseEntry myData = new DatabaseEntry();
			// Open a cursor using a database handle
			if (db != null)
				cursor = db.openCursor(null, null);
			else {
				logger.severe("Problem in getFirst. DB is not running properly.");
				return StorageStatus.FAILURE;
			}
			// Position the cursor
			OperationStatus retVal = cursor.getSearchKey(myKey, myData, LockMode.DEFAULT);

			while (retVal == OperationStatus.SUCCESS) {
				Record rec = Record.parseFrom(myData.getData());
				if (p.test(rec)) {
					result.add(rec);
					return StorageStatus.SUCCESS;
				} else {
					retVal = cursor.getNextDup(myKey, myData, LockMode.DEFAULT);
				}
			}
			cursor.close();
			return StorageStatus.FAILURE;
		} catch (Exception e) {
			logger.severe("Problem in reading key= " + key + " : " + e.toString());
			return StorageStatus.FAILURE;
		} finally {
			if (cursor != null)
				cursor.close();
		}

	}
	
	/**
	 * Reads the all versions of the data item with the given key that satisfy the given predicate.
	 * @param key The key of the data item to read. 
	 * @param p The predicate that need to be satisfied by the versions. 
	 * @param result The list of all versions that satisfy the given predicate. 
	 * @return The result of the operation
	 */
	public StorageStatus readAll(String key, Predicate<Record> p, List<Record> result) {
		Cursor cursor = null;
		try {
			DatabaseEntry myKey = new DatabaseEntry(key.getBytes());
			DatabaseEntry myData = new DatabaseEntry();
			// Open a cursor using a database handle
			if (db != null)
				cursor = db.openCursor(null, null);
			else {
				logger.severe("Problem in getAll. DB is not running properly.");
				return StorageStatus.FAILURE;
			}
			// Position the cursor
			OperationStatus retVal = cursor.getSearchKey(myKey, myData, LockMode.DEFAULT);

			while (retVal == OperationStatus.SUCCESS) {
				Record record = Record.parseFrom(myData.getData());
				if (p.test(record))
					result.add(record);
				else {
					retVal = cursor.getNextDup(myKey, myData, LockMode.DEFAULT);
				}
			}
			cursor.close();
			if (result.size() > 0)
				return StorageStatus.SUCCESS;
			else
				return StorageStatus.FAILURE;
		} catch (Exception e) {
			logger.severe("Problem in reading key= " + key + " : " + e.toString());
			return StorageStatus.FAILURE;
		} finally {
			if (cursor != null)
				cursor.close();
		}
	}

	/**
	 * Runs the storage engine. 
	 * @return The result of the operation
	 */
	public StorageStatus run() {
		try {
			EnvironmentConfig conf = new EnvironmentConfig();
			conf.setAllowCreate(true);

			Utils.checkAndCreateDir(directory);
			env = new Environment(new File(directory), conf);

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setDeferredWrite(true);
			dbConfig.setAllowCreate(true);
			// allowing duplicates
			if (multiVersion) {
				dbConfig.setSortedDuplicates(true);
				Class<?> comparatorClass = Class.forName(comparatorClassName);
				dbConfig.setDuplicateComparator((Comparator<byte[]>) (comparatorClass.newInstance()));
			} else {
				dbConfig.setSortedDuplicates(false);
			}

			db = env.openDatabase(null, name, dbConfig);
			return StorageStatus.SUCCESS;
		} catch (Exception e) {
			logger.severe(Utils.exceptionLogMessge("Problem in running db", e));
			return StorageStatus.FAILURE;
		}
	}


	String setProperty(String defaultValue, String received) {
		if (received != null)
			return received;
		else
			return defaultValue;
	}
}
