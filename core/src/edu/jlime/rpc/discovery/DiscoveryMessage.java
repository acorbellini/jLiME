package edu.jlime.rpc.discovery;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.MessageType;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.util.ByteBuffer;

public class DiscoveryMessage {

	private List<SocketAddress> addresses;

	private UUID id;

	private String name;

	private Map<String, String> additional;

	public DiscoveryMessage(UUID id, String name, Map<String, String> additional, List<SocketAddress> addresses) {
		this.id = id;
		this.name = name;
		this.addresses = addresses;
		this.additional = additional;
	}

	public static DiscoveryMessage fromMessage(Message m) throws UnknownHostException {
		ByteBuffer reader = m.getHeaderBuffer();
		UUID id = reader.getUUID();
		String name = reader.getString();

		ByteBuffer data = m.getDataBuffer();
		Map<String, String> additional = data.getMap();
		int sizeOfAddresses = data.getInt();
		List<SocketAddress> addresses = new ArrayList<>();
		for (int i = 0; i < sizeOfAddresses; i++) {
			String ip = data.getString();
			int port = data.getInt();
			AddressType type = AddressType.fromID(data.get());
			addresses.add(new SocketAddress(new InetSocketAddress(InetAddress.getByName(ip), port), type));
		}
		return new DiscoveryMessage(id, name, additional, addresses);
	}

	public List<SocketAddress> getAddresses() {
		return addresses;
	}

	public void setAddresses(List<SocketAddress> addresses) {
		this.addresses = addresses;
	}

	public UUID getId() {
		return id;
	}

	public void setId(UUID id) {
		this.id = id;
	}

	public Map<String, String> getAdditional() {
		return additional;
	}

	public static Message createNew(MessageType t, Address localID, String name, Map<String, String> discAdditionData,
			List<SocketAddress> addresses) {
		Message ret = Message.newEmptyBroadcastOutDataMessage(t);
		ByteBuffer headerWriter = ret.getHeaderBuffer();
		headerWriter.putUUID(localID.getId());
		headerWriter.putString(name);

		ByteBuffer dataWriter = ret.getDataBuffer();
		dataWriter.putMap(discAdditionData);
		dataWriter.putInt(addresses.size());
		for (SocketAddress isa : addresses) {
			dataWriter.putString(isa.getSockTo().getAddress().getHostAddress());
			dataWriter.putInt(isa.getSockTo().getPort());
			dataWriter.put(isa.getType().getId());
		}
		return ret;
	}

	public String getName() {
		return name;
	}
}