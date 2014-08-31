package edu.jlime.rpc;

import java.util.List;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.SocketAddress;

public interface AddressListProvider {

	public List<SocketAddress> getAddresses();

	public AddressType getType();

	public void updateAddress(Address id, List<SocketAddress> addresses);
}
