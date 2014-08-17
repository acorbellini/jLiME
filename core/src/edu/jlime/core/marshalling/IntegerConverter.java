package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class IntegerConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID) {
		buffer.putInt((Integer) o);
	}

	@Override
	public Object fromArray(ByteBuffer buffer) {
		return buffer.getInt();
	}
}