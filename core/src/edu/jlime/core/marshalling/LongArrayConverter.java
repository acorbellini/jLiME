package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

public class LongArrayConverter implements TypeConverter {

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		long[] arra = (long[]) o;
		buffer.putLongArray(arra);

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return buff.getLongArray();
	}

}
