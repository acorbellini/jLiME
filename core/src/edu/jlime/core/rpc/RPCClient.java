package edu.jlime.core.rpc;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import edu.jlime.core.cluster.Peer;

public class RPCClient {

	protected static ExecutorService async = Executors
			.newCachedThreadPool(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("LocalRPCAsyncThreads");
					t.setDaemon(true);
					return t;
				}
			});

	protected RPCDispatcher disp;
	protected Peer local;
	protected Peer dest;
	protected Peer client;
	protected String targetID;

	public RPCClient(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
		this.disp = disp;
		this.dest = dest;
		this.client = client;
		this.targetID = targetID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dest == null) ? 0 : dest.hashCode());
		result = prime * result
				+ ((targetID == null) ? 0 : targetID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RPCClient other = (RPCClient) obj;
		if (dest == null) {
			if (other.dest != null)
				return false;
		} else if (!dest.equals(other.dest))
			return false;
		if (targetID == null) {
			if (other.targetID != null)
				return false;
		} else if (!targetID.equals(other.targetID))
			return false;
		return true;
	}

}
