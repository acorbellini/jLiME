package edu.jlime.core.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.ClusterChangeListener;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;

public class Client<T, B> implements ClusterChangeListener {

	private List<Peer> cachedPeers = new LinkedList<>();

	private List<T> cachedClients = new LinkedList<>();

	private RPC rpc;

	private ClientFactory<T, B> factory;

	ConcurrentHashMap<Peer, T> clients = new ConcurrentHashMap<>();

	ConcurrentHashMap<T, Peer> peers = new ConcurrentHashMap<>();

	private PeerFilter filter;

	B broadcast = null;

	private Peer client;

	private Logger log = Logger.getLogger(Client.class);

	public Client(RPC rpc, ClientFactory<T, B> factory, PeerFilter filter, Peer client) {
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
						log.debug("Ignoring peer " + peer + " already exists on client manager.");
					return;
				}
				T value = factory.get(peer, client);
				clients.put(peer, value);
				peers.put(value, peer);
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
				if (log.isDebugEnabled())
					log.debug("Client Manager is waiting for " + (min - clients.size()) + " current peers: "
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
		ArrayList<Peer> clientList = new ArrayList<>();
		Enumeration<Peer> e = clients.keys();
		while (e.hasMoreElements()) {
			Peer peer = (Peer) e.nextElement();
			clientList.add(peer);
		}
		Collections.sort(clientList);
		return clientList;
	}

	public T get(Peer peer) {
		return clients.get(peer);
	}

	public RPC getRpc() {
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

	public Peer getLocalPeer() {
		return rpc.getCluster().getLocalPeer();
	}

	public Peer getPeer(T worker) {
		return peers.get(worker);
	}
}
