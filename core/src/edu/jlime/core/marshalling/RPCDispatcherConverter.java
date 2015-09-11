package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.util.ByteBuffer;

public class RPCDispatcherConverter implements TypeConverter {

	private RPC rcp;

	public RPCDispatcherConverter(RPC clp) {
		this.rcp = clp;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID) throws Exception {

	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		return rcp;
	}

}
