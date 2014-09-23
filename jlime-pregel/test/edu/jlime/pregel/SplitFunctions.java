package edu.jlime.pregel;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.SplitFunction;

public class SplitFunctions {

	public static class RoundRobin implements SplitFunction {

		@Override
		public Peer getPeer(Long v, List<Peer> peers) {
			return peers.get((int) (v % peers.size()));
		}

	}

	public static SplitFunction simple() {
		return new RoundRobin();
	}

}
