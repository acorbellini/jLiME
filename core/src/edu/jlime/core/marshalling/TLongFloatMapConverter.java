package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;
import gnu.trove.map.hash.TLongFloatHashMap;

public class TLongFloatMapConverter implements TypeConverter {

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		TLongFloatHashMap map = (TLongFloatHashMap) o;
		buffer.putLongArray(map.keys());
		buffer.putFloatArray(map.values());

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return new TLongFloatHashMap(buff.getLongArray(), buff.getFloatArray());
	}

}
