package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;

public class IntArrayConverter implements TypeConverter {

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		buffer.putByteArray(DataTypeUtils.intArrayToByteArray((int[]) o));

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return DataTypeUtils.byteArrayToIntArray(buff.getByteArray());
	}

}
