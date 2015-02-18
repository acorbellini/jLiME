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

	private PeerFilter filter;

	public Cluster(Peer localPeer, PeerFilter filter) {
		this.localPeer = localPeer;
		this.filter = filter;
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
		synchronized (listeners) {
			listeners.add(list);
		}
	}

	protected void notifyPeerRemoved(Peer peer) {
		synchronized (listeners) {
			for (ClusterChangeListener l : listeners) {
				l.peerRemoved(peer, this);
			}
		}
	};

	protected void notifyPeerAdded(Peer peer) {
		synchronized (listeners) {
			for (ClusterChangeListener l : listeners) {
				l.peerAdded(peer, this);
			}
		}
	};

	public boolean addPeer(Peer peer) {
		if (filter != null && !filter.verify(peer))
			return false;

		synchronized (peers) {
			if (peers.contains(peer))
				return false;
			if (log.isDebugEnabled())
				log.debug(localPeer.getData("app") + ": Added peer "
						+ peer.getName());

			peers.add(peer);
			peers.notifyAll();

			byAddress.put(peer.getAddress(), peer);
			if (!byName.containsKey(peer.getName()))
				byName.put(peer.getName(), new ArrayList<Peer>());
			byName.get(peer.getName()).add(peer);
		}

		notifyPeerAdded(peer);

		if (log.isDebugEnabled())
			log.info(this.toString());
		return true;
	};

	public void removePeer(Peer peer) {

		synchronized (peers) {
			peers.remove(peer);
			byAddress.remove(peer.getAddress());
			byName.get(peer.getName()).remove(peer);

		}

		notifyPeerRemoved(peer);

		if (log.isDebugEnabled())
			log.info(this.toString());
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
		synchronized (peers) {
			for (Peer peer : peers) {
				if (!list.contains(peer)) {
					toRemove.add(peer);
				}
			}
		}

		for (Peer peer : toRemove) {
			removePeer(peer);
		}
	}

	public ArrayList<Peer> getPeers() {
		synchronized (peers) {
			return new ArrayList<>(peers);
		}
	}

	@Override
	public Iterator<Peer> iterator() {
		return getPeers().iterator();
	}

	public boolean waitFor(Peer clientID, long timeToShowup) {
		long init = System.currentTimeMillis();
		synchronized (peers) {
			while (!contains(clientID)) {
				try {
					peers.wait(timeToShowup);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (System.currentTimeMillis() - init > timeToShowup)
					return false;
			}
		}

		return true;
	}

	@Override
	public String toString() {
		HashSet<Peer> copy = null;
		synchronized (peers) {
			copy = new HashSet<>(peers);
		}

		StringBuilder builder = new StringBuilder();
		builder.append("Cluster Status: \n");
		for (Peer p : copy) {
			builder.append(p + "\n");
		}
		return builder.toString();
	}

	public int size() {
		synchronized (peers) {
			return peers.size();
		}
	}

}