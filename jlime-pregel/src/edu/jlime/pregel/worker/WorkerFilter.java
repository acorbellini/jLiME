package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;

public class WorkerFilter implements PeerFilter {
	@Override
	public boolean verify(Peer p) {
		return (p.getData("type").equals("worker"));
	}
}
