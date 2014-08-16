package edu.jlime.core.cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.transport.Address;

public class Cluster implements Iterable<Peer> {

	Logger log = Logger.getLogger(Cluster.class);

	HashSet<Peer> peers = new HashSet<>();

	ConcurrentHashMap<Address, Peer> byAddress = new ConcurrentHashMap<>();

	HashMap<String, List<Peer>> byName = new HashMap<>();

	private List<ClusterChangeListener> listeners = Collections
			.synchronizedList(new ArrayList<ClusterChangeListener>());

	private Peer localPeer;

	public Cluster(Peer localPeer) {
		this.localPeer = localPeer;
		this.addPeer(localPeer);
	}

	public Peer getLocalPeer() {
		return localPeer;
	}

	public List<Peer> getByName(String name) {
		return byName.get(name);
	}

	public boolean contains(Peer p) {
		return peers.contains(p);
	}

	public Peer getByAddress(Address address) {
		return byAddress.get(address);
	}

	public void addChangeListener(ClusterChangeListener list) {
		listeners.add(list);
	}

	protected void notifyPeerRemoved(Peer peer) {
		for (ClusterChangeListener l : listeners) {
			l.peerRemoved(peer, this);
		}
	};

	protected synchronized void notifyPeerAdded(Peer peer) {
		notifyAll();
		for (ClusterChangeListener l : listeners) {
			l.peerAdded(peer, this);
		}
	};

	public void addPeer(Peer peer) {
		if (peers.contains(peer))
			return;

		peers.add(peer);
		byAddress.put(peer.getAddress(), peer);
		if (!byName.containsKey(peer.getName()))
			byName.put(peer.getName(), new ArrayList<Peer>());
		byName.get(peer.getName()).add(peer);

		notifyPeerAdded(peer);
	};

	public void removePeer(Peer peer) {
		if (!peers.contains(peer))
			peers.remove(peer);

		byAddress.remove(peer.getAddress());
		byName.get(peer.getName()).remove(peer);

		notifyPeerRemoved(peer);
		// stop(peer);
	}

	public void update(List<Peer> list) {
		for (Peer srv : list) {
			try {
				addPeer(srv);
			} catch (Exception e) {
				log.error("Error adding server ", e);
			}
		}
		ArrayList<Peer> toRemove = new ArrayList<>();
		for (Peer peer : peers) {
			if (!list.contains(peer)) {
				toRemove.add(peer);
			}
		}
		for (Peer peer : toRemove) {
			removePeer(peer);
		}
	}

	public ArrayList<Peer> getPeers() {
		return new ArrayList<>(peers);
	}

	@Override
	public Iterator<Peer> iterator() {
		return getPeers().iterator();
	}

	public synchronized boolean waitFor(Peer clientID, long timeToShowup) {
		long init = System.currentTimeMillis();
		while (!contains(clientID)) {
			try {
				wait(timeToShowup);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			if (System.currentTimeMillis() - init > timeToShowup)
				return false;
		}
		return true;
	}
}