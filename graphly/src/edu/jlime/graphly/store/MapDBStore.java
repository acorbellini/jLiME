package edu.jlime.graphly.store;

import java.io.File;

import org.apache.log4j.Logger;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class MapDBStore {

	int commit_thresh = 0;

	HTreeMap<Long, byte[]> db;

	private String sPath;

	private String sn;

	private volatile boolean isClosed = false;

	private Logger log = Logger.getLogger(MapDBStore.class);

	private org.mapdb.DB filedb;

	public MapDBStore(String sn, String sPath) {
		this.sPath = sPath;
		this.sn = sn;
	}

	public HTreeMap<Long, byte[]> getDb() throws Exception {
		return getDb(sPath, sn);
	}

	public HTreeMap<Long, byte[]> getDb(String sPath, String sn)
			throws Exception {

		if (db == null) {
			synchronized (this) {
				if (db == null) {

					// org.iq80.leveldb.Logger logger = new
					// org.iq80.leveldb.Logger() {
					// public void log(String message) {
					// if (log.isDebugEnabled())
					// log.debug(message);
					// }
					// };
					// options = new Options();
					// options.logger(logger);
					// options.createIfMissing(true);
					// options.cacheSize(100 * 1024 * 1024);
					// JniDBFactory.pushMemoryPool(100 * 1024 * 1024);

					File dirDB = new File(sPath);
					if (!dirDB.exists())
						dirDB.mkdirs();
					filedb = DBMaker.newFileDB(new File(dirDB + "/" + sn))
							.closeOnJvmShutdown().make();

					db = filedb.createHashMap(sn)
							.keySerializer(Serializer.LONG)
							.valueSerializer(Serializer.BYTE_ARRAY).makeOrGet();
					// db = JniDBFactory.factory.open(dirDB, options);
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
		byte[] res = getDb().get(key);
		return res;
	}

	public void store(long k, byte[] bs) throws Exception {
		HTreeMap<Long, byte[]> db2 = getDb();
		db2.put(k, bs);

		// commit_thresh += (commit_thresh + 1) % 100;
		// if (commit_thresh % 100 == 0) {
		// commit();
		// }
	}

	public void commit() {
		filedb.commit();
	}

	public synchronized void close() {
		if (isClosed || db == null)
			return;
		try {
			db.get(new byte[] { 'a' });
			db.close();
			filedb.commit();
			filedb.close();
			isClosed = true;
		} catch (Exception e) {
			log.error("Error closing LevelDB database", e);
		}

	}

}
