package edu.jlime.collections.intintarray.db;

import java.io.File;
import java.util.List;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class MapDB extends Store {

	private HTreeMap<Integer, byte[]> store;

	private DB db;

	public MapDB(String name, String dir) {
		super(name);
		db = DBMaker.newFileDB(new File(dir + "/" + name)).transactionDisable()
				.mmapFileEnable().closeOnJvmShutdown().make();
		store = db.<Integer, byte[]> getHashMap(name);
	}

	@Override
	public byte[] load(int key) throws Exception {
		return store.get(key);
	}

	@Override
	public void store(int k, byte[] bs) throws Exception {
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

	@Override
	public List<byte[]> loadAll(int[] key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
