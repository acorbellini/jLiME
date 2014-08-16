package edu.jlime.jd;

import edu.jlime.client.JobContext;
import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.job.Job;

public class ClientJob<R> implements Job<R> {

	Job<R> contained;

	Peer client;

	public ClientJob(Job<R> contained, Peer client) {
		this.contained = contained;
		this.client = client;
	}

	@Override
	public R call(JobContext env, JobNode peer) throws Exception {
		return contained.call(env, peer);
	}

	public Peer getClient() {
		return client;
	}

	@Override
	public String toString() {
		return contained.getClass().getName();
	}
}