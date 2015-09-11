package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCObject;
import edu.jlime.util.ByteBuffer;

public class RPCObjectConverter implements TypeConverter {

	private TypeConverters tc;

	public RPCObjectConverter(TypeConverters typeConverters) {
		this.tc = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID) throws Exception {
		this.tc.objectToByteArray(cliID, buffer, null);
		buffer.putString(o.getClass().getName());
		buffer.putByteArray(((RPCObject) o).getByteArray());
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		Peer cli = (Peer) tc.getObjectFromArray(buff);
		String className = buff.getString();
		Class<RPCObject> cl = (Class<RPCObject>) tc.getRPC().loadClass(cli, className);
		RPCObject inst = cl.newInstance();
		return inst.fromByteArray(new ByteBuffer(buff.getByteArray()));
	}
}
