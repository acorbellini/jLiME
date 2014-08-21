package edu.jlime.core.rpc;

import edu.jlime.core.cluster.Peer;

public interface PeerFilter {
	public boolean verify(Peer p);
}
