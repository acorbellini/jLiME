package edu.jlime.collections.intintarray.db;

import java.io.Closeable;
import java.util.List;

import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.util.DataTypeUtils;

public abstract class Store implements Closeable {

	private String name;

	public Store(String name) {
		this.name = name;
	}

	public abstract byte[] load(long key) throws Exception;

	public abstract void store(long k, byte[] bs) throws Exception;

	public String getName() {
		return this.name;
	}

	public static synchronized Store init(StoreConfig storeConfig) {
		StoreFactory loader = new StoreFactory(storeConfig.getType());
		Store ret = loader.getStore(storeConfig.getPath(),
				storeConfig.getStoreName());
		return ret;

	}

	public void store(int k, int[] v) throws Exception {
		store(k, DataTypeUtils.intArrayToByteArray(v));
	}

	public abstract void close();

	public void commit() {
	}

	public String list() throws Exception {
		return "";
	}

	public int size() throws Exception {
		return 0;
	};
}