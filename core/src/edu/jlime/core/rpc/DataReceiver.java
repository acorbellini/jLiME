package edu.jlime.core.rpc;

import edu.jlime.core.transport.Address;

public interface DataReceiver {

	public byte[] process(Address origin, byte[] data);
}
