package edu.jlime.rpc;

import edu.jlime.core.transport.Address;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.np.NetworkProtocol;
import edu.jlime.rpc.tcp.TCP;
import edu.jlime.rpc.udp.UDP;

public abstract class NetworkProtocolFactory {

	private AddressType type;

	public NetworkProtocolFactory(AddressType type) {
		this.type = type;
	}

	public abstract NetworkProtocol getProtocol(String addr);

	public static NetworkProtocolFactory udp(final Address local,
			final Configuration config) {
		return new NetworkProtocolFactory(AddressType.MCAST) {

			@Override
			public NetworkProtocol getProtocol(String addr) {
				return new UDP(local, addr, config.port, config.port_range,
						config.max_msg_size, SocketFactory.getUnicastFactory(
								config.sendBuffer, config.rcvBuffer));
			}

		};
	}

	public static NetworkProtocolFactory tcp(final Address localID,
			final Configuration config) {
		return new NetworkProtocolFactory(AddressType.TCP) {

			@Override
			public NetworkProtocol getProtocol(String addr) {
				return new TCP(localID, addr, config.port, config.port_range,
						config.tcp_config);
			}

		};
	}

	public static NetworkProtocolFactory mcast(final Address local,
			final Configuration config) {
		return new NetworkProtocolFactory(AddressType.UDP) {

			@Override
			public NetworkProtocol getProtocol(String addr) {
				return new UDP(local, config.mcast_addr, config.mcastport,
						config.mcast_port_range, config.max_msg_size, true,
						SocketFactory.getMcastFactory(addr, config.sendBuffer,
								config.rcvBuffer));
			}

		};
	}

}
