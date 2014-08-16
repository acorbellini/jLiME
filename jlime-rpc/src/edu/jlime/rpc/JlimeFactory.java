package edu.jlime.rpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCFactory;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.DiscoveryListener;
import edu.jlime.core.transport.DiscoveryProvider;
import edu.jlime.core.transport.FailureListener;
import edu.jlime.core.transport.FailureProvider;
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

		// STACK
		final Stack commStack = config.getProtocol().equals("tcp") ? Stack
				.tcpStack(config, localAddress, config.name) : Stack.udpStack(
				config, localAddress, config.name);

		// RPC
		jLiMETransport tr = new jLiMETransport(localPeer, commStack);

		RPCDispatcher rpc = new RPCDispatcher(tr);

		return rpc;
	}
}
