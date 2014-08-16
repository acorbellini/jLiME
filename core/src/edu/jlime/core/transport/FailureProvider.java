package edu.jlime.core.transport;

import edu.jlime.core.cluster.Peer;

public interface FailureProvider {

	public void addListener(FailureListener l);

	public void addPeerToMonitor(Peer peer) throws Exception;
}
