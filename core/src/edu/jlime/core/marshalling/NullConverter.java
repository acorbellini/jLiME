package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class NullConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) {
	}

	@Override
	public Object fromArray(ByteBuffer buff) {
		return null;
	}
}