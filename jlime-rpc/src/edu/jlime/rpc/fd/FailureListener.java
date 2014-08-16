package edu.jlime.rpc.fd;

import edu.jlime.core.cluster.Peer;

public interface FailureListener {

	public void nodeFailed(Peer node);
}
