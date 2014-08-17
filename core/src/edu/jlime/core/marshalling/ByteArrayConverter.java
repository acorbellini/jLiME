package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class ByteArrayConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer ByteBuffer, Peer cliID) {
		ByteBuffer.putByteArray((byte[]) o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) {
		return buff.getByteArray();
	}
}