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

	int cacheSize;

	int memPool;

	private Logger log = Logger.getLogger(LocalStore.class);

	public LocalStore(String sPath, int cacheSize, int memPool) {
		this.sPath = sPath;
		this.cacheSize = cacheSize;
		this.memPool = memPool;
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
					options.maxOpenFiles(200);
					options.logger(logger);
					options.createIfMissing(true);
					options.cacheSize(cacheSize);
					JniDBFactory.pushMemoryPool(memPool);

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
		while (true)
			try {
				return getDb().get(key);
			} catch (Exception e) {
				if (!e.getMessage().contains("code: 32"))
					throw e;
			}
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
			load(new byte[] { 'a' });
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
		boolean inclFirst = true;
		boolean first = true;
		byte[] curr = from;
		try {
			boolean done = false;
			while (!done)
				try {
					for (iterator.seek(curr); iterator.hasNext();) {
						e = iterator.next();
						byte[] key = e.getKey();
						curr = key;
						if (UnsignedBytes.lexicographicalComparator().compare(
								to, key) > 0) {
							if (!first || (first && inclFirst))
								cont++;
							first = false;
						} else
							done = true;
					}
					done = true;
				} catch (Exception e1) {
					if (!e1.getMessage().contains("code: 32"))
						throw e1;
					iterator.close();
					iterator = getDb().iterator();
					first = true;
					inclFirst = (cont == 0 && inclFirst);
				}
		} finally {
			iterator.close();
		}
		return cont;
	}

	public List<byte[]> getRangeOfLength(boolean includeFirst, byte[] from,
			byte[] to, int max) throws Exception {
		List<byte[]> ret = new ArrayList<byte[]>();
		byte[] curr = from;
		int cont = 0;
		DBIterator iterator = getDb().iterator();
		Entry<byte[], byte[]> e = null;
		boolean first = true;
		try {
			boolean done = false;
			while (!done)
				try {
					for (iterator.seek(curr); iterator.hasNext();) {
						e = iterator.next();
						byte[] key = e.getKey();
						curr = key;
						if (UnsignedBytes.lexicographicalComparator().compare(
								to, key) > 0) {
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
					done = true;
				} catch (Exception e1) {
					if (!e1.getMessage().contains("code: 32"))
						throw e1;
					iterator.close();
					iterator = getDb().iterator();
					first = true;
					includeFirst = ret.isEmpty() && includeFirst;
				}

		} finally {
			iterator.close();
		}
		return ret;
	}
}
