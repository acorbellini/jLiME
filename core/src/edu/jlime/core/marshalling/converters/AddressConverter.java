package edu.jlime.core.marshalling.converters;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

public class AddressConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) throws Exception {
		buff.putUUID(((Address) o).getId());
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return new Address(buff.getUUID());
	}
}