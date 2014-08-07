package edu.jlime.collections.intintarray.client;

public class DataEntry {

	int key;

	int[] value;

	public DataEntry(int key, int[] value) {
		super();
		this.key = key;
		this.value = value;
	}

	public int getKey() {
		return key;
	}

	public int[] getValue() {
		return value;
	}
}
