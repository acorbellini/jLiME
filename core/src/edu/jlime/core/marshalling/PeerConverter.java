package edu.jlime.core.marshalling;

import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

final class PeerConverter implements TypeConverter {
	/**
	 * 
	 */
	private final TypeConverters typeConverters;

	/**
	 * @param typeConverters
	 */
	PeerConverter(TypeConverters typeConverters) {
		this.typeConverters = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		Peer p = (Peer) o;
		this.typeConverters.objectToByteArray(p.getAddress(), buffer, cliID);
		buffer.putString(p.getName());
		buffer.putMap(p.getDataMap());
	}

	@Override
	public Object fromArray(ByteBuffer buffer) throws Exception {
		Address address = (Address) this.typeConverters.getObjectFromArray(buffer);
		String name = buffer.getString();
		Map<String, String> map = buffer.getMap();
		return new Peer(address, name, map);
	}
}