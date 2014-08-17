package edu.jlime.core.marshalling;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class UUIDConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer ByteBuffer, Peer cliID) {
		ByteBuffer.putUUID((UUID) o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) {
		return buff.getUUID();
	}
}