package edu.jlime.pregel.util;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.SplitFunction;

public class SplitFunctions {

	public static class RoundRobin implements SplitFunction {
		private Peer[] peers;

		@Override
		public Peer getPeer(long v, List<Peer> peers) {
			return peers.get((int) (v % peers.size()));
		}

		@Override
		public void update(List<Peer> peers) throws Exception {
			this.peers = peers.toArray(new Peer[] {});
		}

		@Override
		public Peer[] getPeers() {
			return peers;
		}

		@Override
		public int hash(long v) {
			return (int) (v % peers.length);
		}

	}

	public static SplitFunction rr() {
		return new RoundRobin();
	}

}
