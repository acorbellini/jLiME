package edu.jlime.graphly;

import edu.jlime.core.rpc.RPCObject;
import edu.jlime.util.ByteBuffer;
import gnu.trove.iterator.TLongIntIterator;

import java.io.Serializable;

public class GraphlyCount implements Serializable, RPCObject {
	long[] vids;
	int[] counts;

	public GraphlyCount() {
	}

	public GraphlyCount(long[] vids, int[] counts) {
		this.vids = vids;
		this.counts = counts;
	}

	public TLongIntIterator iterator() {
		return new TLongIntIterator() {
			int c = -1;

			@Override
			public void remove() {
				// TODO Auto-generated method stub

			}

			@Override
			public boolean hasNext() {
				return (c + 1) < vids.length;
			}

			@Override
			public void advance() {
				c++;
			}

			@Override
			public int value() {
				return counts[c];
			}

			@Override
			public int setValue(int val) {
				// TODO Auto-generated method stub
				return 0;
			}

			@Override
			public long key() {
				return vids[c];
			}
		};
	}

	@Override
	public byte[] getByteArray() {
		ByteBuffer buff = new ByteBuffer();
		buff.putLongArray(vids);
		buff.putIntArray(counts);
		return buff.build();
	}

	@Override
	public RPCObject fromByteArray(ByteBuffer buff) {
		vids = buff.getLongArray();
		counts = buff.getIntArray();
		return this;
	}
}
