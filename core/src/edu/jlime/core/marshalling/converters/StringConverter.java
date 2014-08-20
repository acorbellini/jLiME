package edu.jlime.core.marshalling.converters;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.util.ByteBuffer;

public class StringConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) {
		buff.putString((String) o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) {
		return buff.getString();
	}
}