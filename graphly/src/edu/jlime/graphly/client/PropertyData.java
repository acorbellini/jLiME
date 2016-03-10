package edu.jlime.graphly.client;

import java.io.Serializable;

public class PropertyData implements Serializable {

	public long[] keys;
	public Object[] values;

	public PropertyData() {
	}

	public PropertyData(long[] keys, Object[] values) {
		super();
		this.keys = keys;
		this.values = values;
	}

}
