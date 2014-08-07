package edu.jlime.collections.intintarray.client.jobs;

import java.io.Serializable;

import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;

public class StoreConfig implements Serializable {

	private static final long serialVersionUID = 8570935177526256867L;

	StoreType type;

	String path;

	String storeName;

	public StoreType getType() {
		return type;
	}

	public void setType(StoreType type) {
		this.type = type;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getStoreName() {
		return storeName;
	}

	public void setStoreName(String storeName) {
		this.storeName = storeName;
	}

	public StoreConfig(StoreType type, String path, String storeName) {
		super();
		this.type = type;
		this.path = path;
		this.storeName = storeName;
	}

}
