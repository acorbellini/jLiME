package edu.jlime.core.marshalling.converters;

import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

public class PeerConverter implements TypeConverter {
	/**
	 * 
	 */
	private final TypeConverters typeConverters;

	/**
	 * @param typeConverters
	 */
	public PeerConverter(TypeConverters typeConverters) {
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
		Address address = (Address) this.typeConverters
				.getObjectFromArray(buffer);
		String name = buffer.getString();
		Map<String, String> map = buffer.getMap();
		return new Peer(address, name, map);
	}
}