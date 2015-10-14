package edu.jlime.core.transport;

import edu.jlime.core.cluster.Peer;

public interface FailureListener {

	public void nodeFailed(Peer peer);
}
