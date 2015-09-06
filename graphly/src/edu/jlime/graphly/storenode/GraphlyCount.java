package edu.jlime.graphly.storenode;

import java.io.Serializable;

import edu.jlime.core.rpc.RPCObject;
import edu.jlime.util.ByteBuffer;
import gnu.trove.iterator.TLongFloatIterator;

public class GraphlyCount implements Serializable, RPCObject {
	// TLongFloatMap res;
	long[] keys;
	float[] vals;

	public GraphlyCount() {
	}

	public GraphlyCount(long[] k, float[] vals) {
		this.keys = k;
		this.vals = vals;
	}

	public TLongFloatIterator iterator() {
		return new TLongFloatIterator() {
			int i = -1;

			@Override
			public void remove() {

			}

			@Override
			public boolean hasNext() {
				return i + 1 < keys.length;
			}

			@Override
			public void advance() {
				i++;
			}

			@Override
			public float value() {
				return vals[i];
			}

			@Override
			public float setValue(float val) {
				return 0;
			}

			@Override
			public long key() {
				return keys[i];
			}
		};
	}

	@Override
	public byte[] getByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putLongArray(keys);
		buff.putFloatArray(vals);
		return buff.build();
	}

	@Override
	public RPCObject fromByteArray(ByteBuffer buff) {
		keys = buff.getLongArray();
		vals = buff.getFloatArray();
		return this;
	}

	public int size() {
		return keys.length;
	}

	public float[] values() {
		return vals;
	}

	public long[] keys() {
		return keys;
	}

}
