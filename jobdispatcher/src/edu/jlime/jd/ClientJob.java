package edu.jlime.jd;

import edu.jlime.client.JobContext;
import edu.jlime.jd.job.Job;

public class ClientJob<R> implements Job<R> {

	Job<R> contained;

	String clientID;

	public ClientJob(Job<R> contained, String client) {
		this.contained = contained;
		this.clientID = client;
	}

	@Override
	public R call(JobContext env, JobNode peer) throws Exception {
		return contained.call(env, peer);
	}

	public String getClientID() {
		return clientID;
	}

	@Override
	public String toString() {
		return contained.getClass().getName();
	}
}