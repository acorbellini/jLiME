package edu.jlime.rpc.fd;

import edu.jlime.core.cluster.Peer;

public interface FailureProvider {

	public void addFailureListener(FailureListener l);

	public void addPeerToMonitor(Peer peer) throws Exception;
}
