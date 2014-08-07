package edu.jlime.collections.intintarray.db;

public class Persistit extends Store {

	// private String path;

	public Persistit(String name, String path) {
		super(name);
		// this.path = path;
	}

	@Override
	public byte[] load(int key) throws Exception {
		return null;
	}

	@Override
	public void store(int k, byte[] bs) throws Exception {

	}

	@Override
	public void close() {

	}

}
