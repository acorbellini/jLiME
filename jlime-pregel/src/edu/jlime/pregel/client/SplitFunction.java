package edu.jlime.pregel.client;

import java.io.Serializable;
import java.util.List;

import edu.jlime.core.cluster.Peer;

public interface SplitFunction extends Serializable {

	public Peer getPeer(Long o, List<Peer> peers);

}
