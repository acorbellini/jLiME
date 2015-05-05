package edu.jlime.graphly.store;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import com.google.common.primitives.UnsignedBytes;

public class LocalStore {

	private volatile DB db = null;

	private Options options;

	private String sPath;
	private volatile boolean isClosed = false;

	private Logger log = Logger.getLogger(LocalStore.class);

	public LocalStore(String sPath) {
		this.sPath = sPath;
	}

	public DB getDb() throws Exception {
		return getDb(sPath);
	}

	public DB getDb(String sPath) throws Exception {

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
					options.cacheSize(400 * 1024 * 1024);
					JniDBFactory.pushMemoryPool(1024 * 512);

					File dirDB = new File(sPath);
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

	public byte[] load(byte[] key) throws Exception {
		// DataTypeUtils.longToByteArray(key)
		return getDb().get(key);
		// return res;
	}

	public void store(byte[] k, byte[] bs) throws Exception {
		getDb().put(k, bs);
	}

	public synchronized void close() {
		if (isClosed || db == null)
			return;
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
		DBIterator iterator = getDb().iterator();
		Entry<byte[], byte[]> e = null;
		try {
			for (iterator.seek(from); iterator.hasNext();) {
				e = iterator.next();
				if (UnsignedBytes.lexicographicalComparator().compare(to,
						e.getKey()) > 0)
					cont++;
			}
		} finally {
			iterator.close();
		}
		return cont;
	}

	public List<byte[]> getRangeOfLength(boolean includeFirst, byte[] from,
			byte[] to, int max) throws Exception {
		List<byte[]> ret = new ArrayList<byte[]>();
		int cont = 0;
		DBIterator iterator = getDb().iterator();
		Entry<byte[], byte[]> e = null;
		boolean first = true;
		try {
			for (iterator.seek(from); iterator.hasNext();) {
				e = iterator.next();
				if (UnsignedBytes.lexicographicalComparator().compare(to,
						e.getKey()) > 0) {
					if (!first || (first && includeFirst)) {
						ret.add(e.getValue());
						cont++;
					}
					first = false;
					if (cont >= max)
						return ret;
				} else
					return ret;
			}

		} finally {
			iterator.close();
		}
		return ret;
	}
}
