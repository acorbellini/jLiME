package edu.jlime.core.rpc;


public interface RPCFactory {
	public RPCDispatcher build() throws Exception;
}
