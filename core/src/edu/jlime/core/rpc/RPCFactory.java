package edu.jlime.core.rpc;


public interface RPCFactory {
	public RPCDispatcher buildRPC() throws Exception;
}
