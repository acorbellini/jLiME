package edu.jlime.jd.job;

import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;

public abstract class RunJob implements Job<Boolean> {

	private static final long serialVersionUID = 2918381674688422753L;

	@Override
	public final Boolean call(JobContext env, JobNode peer) throws Exception {
		run(env, peer);
		return true;
	}

	public abstract void run(JobContext env, JobNode origin) throws Exception;

}
