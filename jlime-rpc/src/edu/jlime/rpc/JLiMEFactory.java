package edu.jlime.rpc;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCFactory;
import edu.jlime.core.transport.Address;

public class JLiMEFactory implements RPCFactory {

	private Configuration config;

	Logger log = Logger.getLogger(JLiMEFactory.class);

	private Map<String, String> localData;

	private PeerFilter filter;

	public JLiMEFactory(Map<String, String> localData, PeerFilter filter) {
		String configFile = System.getProperty("jlime.config");
		this.config = configFile == null ? new Configuration() : Configuration
				.newConfig(configFile);
		this.localData = localData;
		this.filter = filter;
	}

	public JLiMEFactory(Configuration config, Map<String, String> localData,
			PeerFilter filter) throws Exception {
		this.config = config;
		this.localData = localData;
		this.filter = filter;
	}

	public JLiMEFactory(Configuration config) throws Exception {
		this(config, new HashMap<String, String>(), null);
	}

	@Override
	public RPCDispatcher build() throws Exception {
		Address localAddress = new Address();
		Peer p = new Peer(localAddress, config.name);
		p.putData(localData);

		// STACK
		final Stack commStack = config.getProtocol().equals("tcp") ? Stack
				.tcpStack(config, p.getAddress(), config.name) : Stack
				.udpStack(config, p.getAddress(), config.name);

		// Transport
		jLiMETransport tr = new jLiMETransport(p, filter, commStack);

		// RPC
		RPCDispatcher rpc = new RPCDispatcher(tr);
		return rpc;
	}
}