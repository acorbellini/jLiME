package edu.jlime.core.rpc;

import java.io.Serializable;

import edu.jlime.core.cluster.Peer;

public interface PeerFilter extends Serializable {
	public boolean verify(Peer p);
}
