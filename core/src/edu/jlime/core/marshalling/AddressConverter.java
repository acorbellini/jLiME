package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

final class AddressConverter implements TypeConverter {
	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) throws Exception {
		buff.putUUID(((Address) o).getId());
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return new Address(buff.getUUID());
	}
}