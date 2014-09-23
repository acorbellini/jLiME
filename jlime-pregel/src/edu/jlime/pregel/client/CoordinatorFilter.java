package edu.jlime.pregel.client;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.PeerFilter;

public class CoordinatorFilter implements PeerFilter {
	@Override
	public boolean verify(Peer p) {
		return (p.getData("type").equals("coordinator"));
	}
}