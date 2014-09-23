package edu.jlime.core.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.ClusterChangeListener;
import edu.jlime.core.cluster.Peer;

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
			while (clients.size() < min)
				wait(2000);
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
		ArrayList<Peer> clientList = new ArrayList<>(clients.keySet());
		Collections.sort(clientList);
		return clientList;
	}

	public T get(Peer peer) {
		return clients.get(peer);
	}

}
