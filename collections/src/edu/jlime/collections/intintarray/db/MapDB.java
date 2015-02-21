package edu.jlime.collections.intintarray.db;

import java.io.File;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class MapDB extends Store {

	private HTreeMap<Long, byte[]> store;

	private DB db;

	public MapDB(String name, String dir) {
		super(name);
		db = DBMaker.newFileDB(new File(dir + "/" + name)).transactionDisable()
				.mmapFileEnable().closeOnJvmShutdown().make();
		store = db.<Long, byte[]> getHashMap(name);
	}

	@Override
	public byte[] load(long key) throws Exception {
		return store.get(key);
	}

	@Override
	public void store(long k, byte[] bs) throws Exception {
		store.put(k, bs);
	}

	@Override
	public void close() {
		db.commit();
		store.close();
		db.close();
	}

	@Override
	public void commit() {
		db.commit();
	}

}
