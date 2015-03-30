package edu.jlime.pregel.util;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.SplitFunction;

public class SplitFunctions {

	public static class RoundRobin implements SplitFunction {
		@Override
		public Peer getPeer(long v, List<Peer> peers) {
			return peers.get((int) (v % peers.size()));
		}

		@Override
		public void update() throws Exception {
		}

	}

	public static SplitFunction rr() {
		return new RoundRobin();
	}

}
