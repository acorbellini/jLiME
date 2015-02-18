package edu.jlime.collections.intintarray.db;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import edu.jlime.util.DataTypeUtils;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.procedure.TIntObjectProcedure;

public class LevelDb extends Store {

	private volatile DB db = null;

	private Options options;

	private String sPath;

	private String sn;

	private volatile boolean isClosed = false;

	private Logger log = Logger.getLogger(LevelDb.class);

	public LevelDb(String sn, String sPath) {
		super(sn);
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

	@Override
	public byte[] load(long key) throws Exception {
		byte[] res = getDb().get(DataTypeUtils.longToByteArray(key));
		return res;
	}

	private byte[] intToBytes(int key) {
		return DataTypeUtils.intToByteArray(key);
	}

	public void delete(String k) throws Exception {
		getDb().delete(stringToByteArray(k));
	}

	public void deleteAll(Collection<String> keys) throws Exception {
		for (String k : keys) {
			getDb().delete(stringToByteArray(k));
		}
	}

	private byte[] stringToByteArray(String k) {
		try {
			byte[] bytes = k.getBytes("UTF8");

			return bytes;
		} catch (DBException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return new byte[] {};
	}

	@Override
	public void store(long k, byte[] bs) throws Exception {
		getDb().put(DataTypeUtils.longToByteArray(k), bs);
	}

	public void store(String k, int[] v) throws Exception {
		Arrays.sort(v);
		getDb().put(stringToByteArray(k), DataTypeUtils.intArrayToByteArray(v));
	}

	public void store(TIntObjectHashMap<int[]> map) throws Exception {
		final WriteBatch batch = getDb().createWriteBatch();
		try {
			map.forEachEntry(new TIntObjectProcedure<int[]>() {

				@Override
				public boolean execute(int k, int[] v) {
					batch.put(DataTypeUtils.intToByteArray(k),
							DataTypeUtils.intArrayToByteArray(v));
					return true;
				}
			});
			getDb().write(batch);
		} finally {
			try {
				batch.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
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

	@Override
	public String list() throws Exception {
		log.info("Listing");
		StringBuilder builder = new StringBuilder();
		DBIterator it = getDb().iterator();

		try {
			it.seekToFirst();
			while (it.hasNext()) {
				Entry<byte[], byte[]> next = it.next();
				builder.append(DataTypeUtils.byteArrayToInt(next.getKey())
						+ " "
						+ DataTypeUtils.byteArrayToIntArray(next.getValue())
						+ "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			it.close();
		}
		return builder.toString();

	}

	@Override
	public int size() throws Exception {
		log.info("Getting Size");
		int i = 0;
		DBIterator it = getDb().iterator();

		try {
			it.seekToFirst();
			while (it.hasNext()) {
				Entry<byte[], byte[]> next = it.next();
				i++;
				// builder.append(DataTypeUtils.byteArrayToInt(next.getKey())
				// + " "
				// + DataTypeUtils.byteArrayToIntArray(next.getValue())
				// + "\n");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			it.close();
		}
		return i;

	}
}
