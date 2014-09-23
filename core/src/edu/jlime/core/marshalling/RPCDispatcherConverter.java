package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.util.ByteBuffer;

public class RPCDispatcherConverter implements TypeConverter {

	private RPCDispatcher rcp;

	public RPCDispatcherConverter(RPCDispatcher clp) {
		this.rcp = clp;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return rcp;
	}

}
