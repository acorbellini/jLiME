package edu.jlime.rpc.discovery;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.IP;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.message.AddressType;
import edu.jlime.rpc.message.Address;
import edu.jlime.rpc.message.Message;
import edu.jlime.rpc.message.SocketAddress;
import edu.jlime.rpc.message.MessageProcessor;
import edu.jlime.util.NetworkUtils.SelectedInterface;

public class PingDiscovery extends Discovery {

	private int port;

	private int port_range;

	public PingDiscovery(UUID local, MessageProcessor unicast,
			Configuration opts, int port, int port_range) throws Exception {
		super(local, opts, unicast, unicast);
		this.port = port;
		this.port_range = port_range;
	}

	@Override
	protected synchronized void startDiscovery(List<SelectedInterface> added) {
		for (SelectedInterface selectedInterface : added) {
			// if (selectedInterface.getInet() instanceof Inet4Address) {
			IP ip = IP.toIP(selectedInterface.getInet().getHostAddress());
			for (int i = 1; i < ip.maxDirValue(); i++) {
				ip.setLast(i);
				for (int p = port; p < port + port_range; p++) {
					try {
						sendDiscoveryMessage(ip.toString(), p);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
			// }
		}
	}

	private void sendDiscoveryMessage(String addrToLookFor, int p)
			throws Exception {
		Message msg = newDiscoveryMessage();
		msg.setTo(new SocketAddress(Address.noAddr(), new InetSocketAddress(
				InetAddress.getByName(addrToLookFor), p), AddressType.ANY));
		discoveryData.queue(msg);
	}

	@Override
	public void cleanupOnFailedPeer(Address peer) {

	}

	@Override
	public void setMetrics(Metrics metrics) {

	}
}
