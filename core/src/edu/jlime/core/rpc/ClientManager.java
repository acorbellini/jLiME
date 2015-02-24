package edu.jlime.core.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.ClusterChangeListener;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;

public class ClientManager<T, B> implements ClusterChangeListener {

	private List<Peer> cachedPeers = new LinkedList<>();

	private List<T> cachedClients = new LinkedList<>();

	private RPCDispatcher rpc;

	private ClientFactory<T, B> factory;

	SortedMap<Peer, T> clients = Collections
			.synchronizedSortedMap(new TreeMap<Peer, T>());

	private PeerFilter filter;

	B broadcast = null;

	private Peer client;

	private Logger log = Logger.getLogger(ClientManager.class);

	public ClientManager(RPCDispatcher rpc, ClientFactory<T, B> factory,
			PeerFilter filter, Peer client) {
		this.rpc = rpc;
		this.filter = filter;
		this.factory = factory;
		this.client = client;

		this.rpc.getCluster().addChangeListener(this);

		for (Peer p : rpc.getCluster()) {
			peerAdded(p, rpc.getCluster());
		}
	}

	@Override
	public void peerRemoved(Peer peer, Cluster c) {
		synchronized (this) {
			clients.remove(peer);
			cachedPeers = buildCachedPeers();
			cachedClients = buildClientList();
			broadcast = factory.getBroadcast(cachedPeers, c.getLocalPeer());
		}
	}

	@Override
	public void peerAdded(Peer peer, Cluster c) {
		if (filter.verify(peer)) {
			synchronized (this) {
				if (log.isDebugEnabled())
					log.debug("Adding peer " + peer + " to client manager.");
				if (clients.containsKey(peer)) {
					if (log.isDebugEnabled())
						log.debug("Ignoring peer " + peer
								+ " already exists on client manager.");
					return;
				}
				clients.put(peer, factory.get(peer, client));
				notify();
				cachedPeers = buildCachedPeers();
				cachedClients = buildClientList();
				broadcast = factory.getBroadcast(cachedPeers, client);
			}
		}
	}

	public List<T> getAll() {
		return cachedClients;
	}

	private List<T> buildClientList() {
		ArrayList<T> clientList = new ArrayList<>(clients.values());
		Collections.sort(clientList, new Comparator<T>() {

			@Override
			public int compare(T o1, T o2) {
				RPCClient cli = (RPCClient) o1;
				RPCClient cli2 = (RPCClient) o2;
				return cli.dest.compareTo(cli2.dest);
			}

		});
		return clientList;
	}

	public T getFirst() {
		return clients.values().iterator().next();
	}

	public T waitFirst() throws Exception {
		return waitForClient(1).get(0);
	}

	public List<T> waitForClient(int min) throws Exception {
		synchronized (this) {
			while (clients.size() < min) {
				log.info("Client Manager is waiting for "
						+ (min - clients.size()) + " current peers: "
						+ clients.keySet());
				wait(2000);
			}
		}

		return getAll();

	}

	public B broadcast() {
		return broadcast;
	}

	public List<Peer> getPeers() {
		return cachedPeers;
	}

	private List<Peer> buildCachedPeers() {
		synchronized (this) {
			ArrayList<Peer> clientList = new ArrayList<>(clients.keySet());
			Collections.sort(clientList);
			return clientList;
		}
	}

	public T get(Peer peer) {
		return clients.get(peer);
	}

	public RPCDispatcher getRpc() {
		return rpc;
	}

	public T get(PeerFilter f) {
		for (Entry<Peer, T> t : clients.entrySet()) {
			if (f.verify(t.getKey()))
				return t.getValue();
		}
		return null;
	}

	public Map<Peer, T> getMap() {
		return clients;
	}
}
