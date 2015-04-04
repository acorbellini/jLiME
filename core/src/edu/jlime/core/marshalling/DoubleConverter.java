package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

public class DoubleConverter implements TypeConverter {

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		buffer.putDouble((Double) o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return buff.getDouble();
	}

}
