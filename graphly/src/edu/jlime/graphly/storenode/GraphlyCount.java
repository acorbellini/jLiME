package edu.jlime.graphly.storenode;

import java.io.Serializable;

import edu.jlime.core.rpc.RPCObject;
import edu.jlime.util.ByteBuffer;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.hash.TLongFloatHashMap;

public class GraphlyCount implements Serializable, RPCObject {
	TLongFloatHashMap res;

	public GraphlyCount() {
	}

	public GraphlyCount(TLongFloatHashMap res) {
		this.res = res;
	}

	public TLongFloatHashMap getRes() {
		return res;
	}

	@Override
	public byte[] getByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putLongArray(res.keys());
		buff.putFloatArray(res.values());
		return buff.build();
	}

	@Override
	public RPCObject fromByteArray(ByteBuffer buff) {
		res = new TLongFloatHashMap(buff.getLongArray(), buff.getFloatArray());
		return this;
	}
}
