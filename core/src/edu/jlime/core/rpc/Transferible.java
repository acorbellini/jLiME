package edu.jlime.core.rpc;

import java.io.Serializable;

public interface Transferible extends Serializable {
	public void setRPC(RPCDispatcher rpc);
}
