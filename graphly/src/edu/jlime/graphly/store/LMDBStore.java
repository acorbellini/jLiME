package edu.jlime.graphly.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.fusesource.lmdbjni.BufferCursor;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Env;

import com.google.common.primitives.UnsignedBytes;

public class LMDBStore {

	private volatile Database db = null;

	private String sPath;

	private String sn;

	private volatile boolean isClosed = false;

	private Logger log = Logger.getLogger(LMDBStore.class);

	private Env env;

	public LMDBStore(String sn, String sPath) {
		this.sPath = sPath;
		this.sn = sn;
	}

	public Database getDb() throws Exception {
		return getDb(sPath, sn);
	}

	public Database getDb(String sPath, String sn) throws Exception {

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

					String path = sPath;
					File dirDB = new File(path);
					if (!dirDB.exists())
						dirDB.mkdirs();
					// db = JniDBFactory.factory.open(dirDB, options);
					// db.compactRange(null, null);

					this.env = new Env(path);
					this.db = env.openDatabase(sn);

					if (log.isDebugEnabled())
						log.debug("Opened.");
				}
			}
		}

		if (isClosed)
			throw new Exception("DB is closed");

		return db;
	}

	public byte[] load(byte[] key) throws Exception {
		// DataTypeUtils.longToByteArray(key)
		return getDb().get(key);
		// return res;
	}

	public void store(byte[] k, byte[] bs) throws Exception {
		synchronized (LMDBStore.class) {
			getDb().put(k, bs);
		}
	}

	public synchronized void close() {
		if (isClosed || db == null)
			return;

		try {
			env.close();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		try {
			// Test for being open
			db.get(new byte[] { 'a' });
			db.close();
			isClosed = true;
		} catch (Exception e) {
			log.error("Error closing LevelDB database", e);
		}

	}

	public int count(byte[] from, byte[] to) throws Exception {
		int cont = 0;
		try (BufferCursor cursor = db.bufferCursor()) {
			cursor.seek(from);
			while (cursor.next()) {
				byte[] k = cursor.keyBytes();
				if (UnsignedBytes.lexicographicalComparator().compare(to, k) > 0)
					cont++;
			}

		}
		return cont;
	}

	public List<byte[]> getRangeOfLength(boolean includeFirst, byte[] from,
			byte[] to, int max) throws Exception {
		List<byte[]> ret = new ArrayList<byte[]>();
		int cont = 0;
		boolean first = true;
		try (BufferCursor cursor = db.bufferCursor()) {
			cursor.seek(from);
			while (cursor.next()) {
				byte[] k = cursor.keyBytes();
				if (UnsignedBytes.lexicographicalComparator().compare(to, k) > 0) {
					if (!first || (first && includeFirst)) {
						byte[] v = cursor.valBytes();
						ret.add(v);
					}
					first = false;
					cont++;
					if (cont >= max)
						return ret;
				} else
					return ret;
			}
		}
		return ret;
	}
}
