package edu.jlime.rpc;

import edu.jlime.rpc.ProtocolHost.ProtocolHandle;

public interface HandleListener<T> {
	public void added(ProtocolHandle<T> handle);
}
