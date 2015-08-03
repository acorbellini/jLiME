package edu.jlime.pregel.queues;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.UUID;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

import edu.jlime.util.DataTypeUtils;
import edu.jlime.util.FileUtils;
import gnu.trove.iterator.TLongFloatIterator;

public class BerkeleyKV {

	private static final long CACHE = 100 * 1024 * 1024;

	private static final String DB_NAME = "Berkeley";

	private static final float NO_ENTRY = Float.MIN_VALUE;

	Environment myDbEnvironment = null;
	Database myDatabase = null;

	private File file;

	public BerkeleyKV() {
		createDB();
	}

	private void createDB() {
		try {
			// Open the environment, creating one if it does not exist
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			envConfig.setCacheSize(CACHE);
			String path = System.getProperty("java.io.tmpdir")
					+ "/graphly-berkeley-" + UUID.randomUUID() + "/";
			this.file = new File(path);
			if (!file.exists())
				file.mkdir();
			myDbEnvironment = new Environment(file, envConfig);

			// Open the database, creating one if it does not exist
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			myDatabase = myDbEnvironment.openDatabase(null, DB_NAME, dbConfig);
		} catch (DatabaseException dbe) {
			dbe.printStackTrace();
		}
	}

	public float get(final Long k) {
		DatabaseEntry kGet = new DatabaseEntry();
		DatabaseEntry vGet = new DatabaseEntry();

		kGet.setData(DataTypeUtils.longToByteArray(k));
		OperationStatus stat;
		try {
			stat = myDatabase.get(null, kGet, vGet, LockMode.DEFAULT);
		} catch (DatabaseException e) {
			e.printStackTrace();
			return NO_ENTRY;
		}
		if (stat == OperationStatus.NOTFOUND)
			return NO_ENTRY;
		return Float
				.intBitsToFloat(DataTypeUtils.byteArrayToInt(vGet.getData()));

	}

	public int size() {
		try {
			return (int) myDatabase.count();
		} catch (DatabaseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public void put(long to, float msg) {
		DatabaseEntry k = new DatabaseEntry();
		DatabaseEntry v = new DatabaseEntry();

		k.setData(DataTypeUtils.longToByteArray(to));
		v.setData(DataTypeUtils.intToByteArray(Float.floatToIntBits(msg)));
		try {
			myDatabase.put(null, k, v);
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
	}

	public TLongFloatIterator iterator() {
		try {
			final Cursor cursor = myDatabase.openCursor(null, null);
			return new TLongFloatIterator() {
				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry data = new DatabaseEntry();

				@Override
				public void remove() {
				}

				@Override
				public boolean hasNext() {
					OperationStatus stat = null;
					try {
						stat = cursor.getNext(key, data, LockMode.DEFAULT);
					} catch (DatabaseException e) {
						e.printStackTrace();
					}

					return stat != null && stat == OperationStatus.SUCCESS;
				}

				@Override
				public void advance() {
				}

				@Override
				public float value() {
					return Float.intBitsToFloat(DataTypeUtils
							.byteArrayToInt(data.getData()));
				}

				@Override
				public float setValue(float val) {
					return 0;
				}

				@Override
				public long key() {
					return DataTypeUtils.byteArrayToLong(key.getData());
				}
			};
		} catch (DatabaseException e) {
			e.printStackTrace();
			return null;
		}

	}

	public void clear() {
		try {
			myDatabase.close();
			myDbEnvironment.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
		}
		try {
			FileUtils.deleteRecursive(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		createDB();
	}

	public float getNoEntryValue() {
		return NO_ENTRY;
	}
}