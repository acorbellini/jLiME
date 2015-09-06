package edu.jlime.pregel.graph;

import java.util.Comparator;

import edu.jlime.util.ByteBuffer;

public class LongLongComparator implements Comparator<byte[]> {

	@Override
	public int compare(byte[] o1, byte[] o2) {
		ByteBuffer b1 = new ByteBuffer(o1);
		long k1 = b1.getLong();
		long v1 = b1.getLong();
		ByteBuffer b2 = new ByteBuffer(o2);
		long k2 = b2.getLong();
		long v2 = b2.getLong();

		int compare = Long.compare(k1, k2);
		if (compare == 0)
			compare = Long.compare(v1, v2);

		return compare;
	}

}
