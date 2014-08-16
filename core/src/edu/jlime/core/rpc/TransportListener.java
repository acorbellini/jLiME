package edu.jlime.core.rpc;

import edu.jlime.core.transport.Address;

public interface TransportListener {

	public byte[] process(Address origin, byte[] data);
}
