package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class BooleanConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) {
		buff.putBoolean((Boolean) o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) {
		return buff.getBoolean();
	}
}