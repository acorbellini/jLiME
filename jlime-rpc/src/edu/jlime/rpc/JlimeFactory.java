package edu.jlime.rpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCFactory;
import edu.jlime.core.transport.Address;
import edu.jlime.rpc.discovery.DiscoveryListener;
import edu.jlime.rpc.discovery.DiscoveryProvider;
import edu.jlime.rpc.fd.FailureListener;
import edu.jlime.rpc.fd.FailureProvider;
import edu.jlime.rpc.message.JLiMEAddress;

public class JlimeFactory implements RPCFactory {

	private Configuration config;

	Logger log = Logger.getLogger(JlimeFactory.class);

	private Map<String, String> localData;

	public JlimeFactory(Configuration config, Map<String, String> localData)
			throws Exception {
		this.config = config;
		this.localData = localData;
	}

	public JlimeFactory(Configuration config) throws Exception {
		this(config, new HashMap<String, String>());
	}

	@Override
	public RPCDispatcher build() throws Exception {

		JLiMEAddress localAddress = new JLiMEAddress();

		Peer localPeer = new Peer(localAddress, config.name);
		localPeer.putData(localData);

		// CLUSTER
		final Cluster cluster = new Cluster(localPeer);

		// STACK
		final Stack commStack = config.getProtocol().equals("tcp") ? Stack
				.tcpStack(config, localAddress, config.name) : Stack.udpStack(
				config, localAddress, config.name);

		// RPC
		jLiMETransport tr = new jLiMETransport(commStack);

		RPCDispatcher rpc = new RPCDispatcher(cluster);
		
		rpc.setTransport(tr);

		DiscoveryProvider disco = commStack.getDiscovery();
		disco.putData(localPeer.getDataMap());

		final FailureProvider fail = commStack.getFailureDetection();

		// DISCOVERY
		disco.addListener(new DiscoveryListener() {

			@Override
			public synchronized void memberMessage(Address from, String name,
					Map<String, String> data) throws Exception {
				Peer p = cluster.getByAddress(from);
				if (p == null) {
					log.info("New member found : " + name + " id " + from);
					Peer peer = new Peer(from, name);

					peer.putData(data);
					cluster.addPeer(peer);
					fail.addPeerToMonitor(peer);
				}

			}
		});

		fail.addFailureListener(new FailureListener() {
			@Override
			public void nodeFailed(Peer peer) {
				log.info("Node " + peer + " crashed. ");
				cluster.removePeer(peer);
				commStack.cleanupOnFailedPeer((JLiMEAddress) peer.getAddress());
			}
		});
		return rpc;
	}
}
