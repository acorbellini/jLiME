package edu.jlime.graphly.storenode.rpc;

import java.io.Serializable;

public class AdjacencyData implements Serializable {
	public long[] keys;
	public long[][] values;

	public AdjacencyData() {
	}

	public AdjacencyData(long[] keys, long[][] values) {
		super();
		this.keys = keys;
		this.values = values;
	}

}
