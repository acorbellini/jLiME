package edu.jlime.core.rpc;

import java.util.List;

import edu.jlime.core.cluster.Peer;

public interface ClientFactory<T, B> {
	public T get(Peer to, Peer client);

	public B getBroadcast(List<Peer> to, Peer client);
}
