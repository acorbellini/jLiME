package edu.jlime.rpc.discovery;

import java.util.UUID;

import edu.jlime.rpc.message.SocketAddress;

public interface AddressTester {

	public boolean test(UUID id, SocketAddress socketAddress);

}
