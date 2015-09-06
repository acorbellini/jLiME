package edu.jlime.pregel.client;

import java.io.Serializable;
import java.util.List;

import edu.jlime.core.cluster.Peer;

public interface SplitFunction extends Serializable {

	public Peer getPeer(long v, List<Peer> peers);

	public void update(List<Peer> peers) throws Exception;

	public Peer[] getPeers();

	public int hash(long to);

}
