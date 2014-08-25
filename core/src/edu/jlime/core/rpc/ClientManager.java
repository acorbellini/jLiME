package edu.jlime.core.rpc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.ClusterChangeListener;
import edu.jlime.core.cluster.Peer;

public class ClientManager<T, B> implements ClusterChangeListener {

	private RPCDispatcher rpc;
	private ClientFactory<T, B> factory;

	SortedMap<Peer, T> clients = Collections
			.synchronizedSortedMap(new TreeMap<Peer, T>());

	private PeerFilter filter;

	public ClientManager(RPCDispatcher rpc, ClientFactory<T, B> factory,
			PeerFilter filter) {
		this.rpc = rpc;
		this.filter = filter;
		this.factory = factory;
		this.rpc.getCluster().addChangeListener(this);
		for (Peer p : rpc.getCluster()) {
			peerAdded(p, rpc.getCluster());
		}

	}

	@Override
	public void peerRemoved(Peer peer, Cluster c) {
		clients.remove(peer);
	}

	@Override
	public void peerAdded(Peer peer, Cluster c) {
		if (filter.verify(peer)) {
			synchronized (this) {
				clients.put(peer, factory.get(peer, c.getLocalPeer()));
				notify();
			}
		}
	}

	public List<T> getAll() {
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

	public T first() {
		return clients.values().iterator().next();
	}

	public T waitFirst() throws Exception {
		return wait(1).get(0);
	}

	public List<T> wait(int min) throws Exception {
		synchronized (this) {
			while (clients.size() < min)
				wait();
		}

		return getAll();

	}

	public B broadcast() {
		return factory.getBroadcast(rpc.getCluster().getPeers(), rpc
				.getCluster().getLocalPeer());
	}

}
