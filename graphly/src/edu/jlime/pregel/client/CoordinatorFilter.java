package edu.jlime.pregel.client;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.pregel.coordinator.CoordinatorServer;

public class CoordinatorFilter implements PeerFilter {
	@Override
	public boolean verify(Peer p) {
		return (p.getData("app").contains(CoordinatorServer.COORDINATOR_KEY));
	}
}