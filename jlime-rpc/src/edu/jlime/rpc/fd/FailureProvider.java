package edu.jlime.rpc.fd;

import edu.jlime.rpc.PeerJlime;

public interface FailureProvider {

	public void addFailureListener(FailureListener l);

	public void addPeerToMonitor(PeerJlime peer) throws Exception;
}
