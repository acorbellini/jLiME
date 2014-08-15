package edu.jlime.jd;

import java.io.Serializable;
import java.util.UUID;

import edu.jlime.client.JobContext;
import edu.jlime.jd.job.Job;

public class RemoteReference<T> implements Serializable {

	private String key;
	private JobNode node;

	public RemoteReference(T adyacents, JobContext ctx) {
		this.node = ctx.getCluster().getLocalNode();
		this.key = "RemoteReference-" + UUID.randomUUID();
		ctx.put(this.key, adyacents);

	}

	public RemoteReference(JobNode node, String key) {
		this.key = key;
		this.node = node;
	}

	public String getKey() {
		return key;
	}

	public JobNode getNode() {
		return node;
	}

	public T get() throws Exception {
		return node.exec(new Job<T>() {

			@Override
			public T call(JobContext env, JobNode peer) throws Exception {
				return (T) env.get(key);
			}

		});
	}
}