package edu.jlime.core.cluster;

public interface ClusterChangeListener {

	public void peerRemoved(Peer peer, Cluster c);

	public void peerAdded(Peer peer, Cluster c);
}
