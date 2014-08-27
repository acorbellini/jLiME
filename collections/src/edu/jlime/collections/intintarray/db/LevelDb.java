package edu.jlime.collections.intintarray.db;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.ArrayUtils;
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
					options = new Options();
					options.createIfMissing(true);
					options.cacheSize(100 * 1024 * 1024);
					JniDBFactory.pushMemoryPool(100 * 1024 * 1024);

					File dirDB = new File(sPath + "/" + sn);
					if (!dirDB.exists())
						dirDB.mkdirs();
					db = JniDBFactory.factory.open(dirDB, options);
					Logger.getLogger(LevelDb.class).info("Opened.");
				}
			}
		}

		if (isClosed)
			throw new Exception("DB is closed");

		return db;
	}

	@Override
	public byte[] load(int key) throws Exception {
		byte[] res = getDb().get(DataTypeUtils.intToByteArray(key));
		return res;
	}

	@Override
	public List<byte[]> loadAll(int[] k) throws Exception {
		log.info("Sorting input list");
		Integer[] sorted = ArrayUtils.toObject(k);
		Arrays.sort(sorted, new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				byte[] b1 = DataTypeUtils.intToByteArray(o1);
				byte[] b2 = DataTypeUtils.intToByteArray(o2);
				for (int i = 0; i < 4; i++) {
					int comp = Byte.compare(b1[i], b2[i]);
					if (comp != 0)
						return comp;
				}
				return 0;
			}
		});
		log.info("Sorted input list");
		List<byte[]> res = new ArrayList<byte[]>();
		System.out.println("Loading  " + sorted.length);
		DBIterator it = getDb().iterator();

		// if(key.length==1){
		// System.out.println("Key[0]: " + load(key[0]));
		// }
		try {
			for (int i = 0; i < sorted.length; i++) {
				it.seek(intToBytes(sorted[i]));
				if (!it.hasNext())
					break;
				Entry<byte[], byte[]> e = it.peekNext();
				if (DataTypeUtils.byteArrayToInt(e.getKey()) == sorted[i])
					res.add(e.getValue());

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			it.close();
		}
		System.out.println("Returning" + res.size());
		return res;
	}

	private byte[] intToBytes(int key) {
		return DataTypeUtils.intToByteArray(key);
	}

	public int[] load(String key) throws DBException, Exception {
		byte[] res = getDb().get(stringToByteArray(key));
		if (res != null)
			return DataTypeUtils.byteArrayToIntArray(res);
		else
			return null;
	}

	public Map<String, int[]> loadAll(Collection<String> keys) throws Exception {
		Map<String, int[]> all = new HashMap<String, int[]>();
		for (String k : keys) {
			all.put(k, load(k));
		}
		return all;
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
	public void store(int k, byte[] bs) throws Exception {
		getDb().put(DataTypeUtils.intToByteArray(k), bs);
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
