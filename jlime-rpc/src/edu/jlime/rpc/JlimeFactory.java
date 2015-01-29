package edu.jlime.rpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCFactory;
import edu.jlime.core.server.TransportFactory;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Transport;

public class JLiMEFactory implements RPCFactory, TransportFactory {

	private Configuration config;

	Logger log = Logger.getLogger(JLiMEFactory.class);

	private Map<String, String> localData;

	public JLiMEFactory() {
		String configFile = System.getProperty("jlime.config");
		this.config = configFile == null ? new Configuration() : Configuration
				.newConfig(configFile);

	}

	public JLiMEFactory(Configuration config, Map<String, String> localData)
			throws Exception {
		this.config = config;
		this.localData = localData;
	}

	public JLiMEFactory(Configuration config) throws Exception {
		this(config, new HashMap<String, String>());
	}

	@Override
	public RPCDispatcher buildRPC() throws Exception {
		Address localAddress = new Address();
		Peer localPeer = new Peer(localAddress, config.name);
		localPeer.putData(localData);
		// RPC
		RPCDispatcher rpc = new RPCDispatcher(build(localPeer));
		return rpc;
	}

	@Override
	public Transport build(Peer p) {

		// STACK
		final Stack commStack = config.getProtocol().equals("tcp") ? Stack
				.tcpStack(config, p.getAddress(), config.name) : Stack
				.udpStack(config, p.getAddress(), config.name);

		// Transport
		jLiMETransport tr = new jLiMETransport(p, commStack);
		return tr;
	}
}
