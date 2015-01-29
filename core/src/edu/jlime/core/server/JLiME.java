package edu.jlime.core.server;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.transport.Address;

public class JLiME {
	private TransportFactory factory;

	private Map<String, String> data;

	private RPCDispatcher rpc;

	private Peer localPeer;

	public JLiME(TransportFactory factory, Map<String, String> localData) {
		this.factory = factory;
		this.data = localData;
	}

	public static void main(String[] args) throws Exception {

		String transport = args[0];
		String data = "";
		if (args.length >= 2)
			data = args[1];

		Map<String, String> localData = new HashMap<>();

		for (String string : data.split(",")) {
			int indexOf = string.indexOf("=");
			localData.put(string.substring(0, indexOf),
					string.substring(indexOf + 1, string.length()));
		}

		TransportFactory factory = (TransportFactory) Class.forName(transport)
				.newInstance();

		new JLiME(factory, localData).start();

	}

	public void start() throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, UnknownHostException, Exception {

		Address localAddress = new Address();

		localPeer = new Peer(localAddress, InetAddress.getLocalHost()
				.getHostName());

		localPeer.putData(data);

		rpc = new RPCDispatcher(factory.build(localPeer));

		rpc.start();

		Logger log = Logger.getLogger(JLiME.class);

		log.info("Started RPC registry on " + localPeer + " with address "
				+ rpc.getRealAddress() + " with data " + data);
	}

	public RPCDispatcher getRpc() {
		return rpc;
	}
}
