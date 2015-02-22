package edu.jlime.graphly.store;

import java.io.File;

import org.apache.log4j.Logger;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import edu.jlime.util.DataTypeUtils;

public class LocalStore {

	private volatile DB db = null;

	private Options options;

	private String sPath;

	private String sn;

	private volatile boolean isClosed = false;

	private Logger log = Logger.getLogger(LocalStore.class);

	public LocalStore(String sn, String sPath) {
		this.sPath = sPath;
		this.sn = sn;
	}

	public DB getDb() throws Exception {
		return getDb(sPath, sn);
	}

	public DB getDb(String sPath, String sn) throws Exception {

		if (db == null) {
			synchronized (this) {
				if (db == null) {

					org.iq80.leveldb.Logger logger = new org.iq80.leveldb.Logger() {
						public void log(String message) {
							if (log.isDebugEnabled())
								log.debug(message);
						}
					};
					options = new Options();
					options.logger(logger);
					options.createIfMissing(true);
					options.cacheSize(100 * 1024 * 1024);
					JniDBFactory.pushMemoryPool(100 * 1024 * 1024);

					File dirDB = new File(sPath + "/" + sn);
					if (!dirDB.exists())
						dirDB.mkdirs();
					db = JniDBFactory.factory.open(dirDB, options);
					// db.compactRange(null, null);
					if (log.isDebugEnabled())
						log.debug("Opened.");
				}
			}
		}

		if (isClosed)
			throw new Exception("DB is closed");

		return db;
	}

	public byte[] load(long key) throws Exception {
		byte[] res = getDb().get(DataTypeUtils.longToByteArray(key));
		return res;
	}

	public void store(long k, byte[] bs) throws Exception {
		getDb().put(DataTypeUtils.longToByteArray(k), bs);
	}

	public synchronized void close() {
		if (isClosed || db == null)
			return;
		try {
			db.get(new byte[] { 'a' });
			db.close();
			isClosed = true;
		} catch (Exception e) {
			log.error("Error closing LevelDB database", e);
		}

	}

}
