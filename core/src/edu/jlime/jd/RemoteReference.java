package edu.jlime.jd;

import java.io.Serializable;
import java.util.UUID;

import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class RemoteReference<T> implements Serializable {

	private String key;
	private Node node;
	private boolean removeOnGet = false;

	public RemoteReference(T adyacents, JobContext ctx) {
		this(adyacents, ctx, false);
	}

	public RemoteReference(T adyacents, JobContext ctx, boolean removeOnGet) {
		this.node = ctx.getCluster().getLocalNode();
		this.key = "RemoteReference-" + UUID.randomUUID();
		ctx.put(this.key, adyacents);

	}

	public RemoteReference(Node node, String key) {
		this.key = key;
		this.node = node;
	}

	public String getKey() {
		return key;
	}

	public Node getNode() {
		return node;
	}

	public T get() throws Exception {
		return node.exec(new Job<T>() {
			@Override
			public T call(JobContext env, Node peer) throws Exception {
				if (removeOnGet)
					return (T) env.remove(key);
				return (T) env.get(key);
			}

		});
	}
}
