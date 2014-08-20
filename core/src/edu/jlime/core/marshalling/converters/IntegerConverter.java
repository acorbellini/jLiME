package edu.jlime.core.marshalling.converters;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.util.ByteBuffer;

public class IntegerConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID) {
		buffer.putInt((Integer) o);
	}

	@Override
	public Object fromArray(ByteBuffer buffer) {
		return buffer.getInt();
	}
}