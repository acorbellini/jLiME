package edu.jlime.jd.job;

import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;

public abstract class RunJob implements Job<Boolean> {

	private static final long serialVersionUID = 2918381674688422753L;

	@Override
	public final Boolean call(JobContext env, Node peer) throws Exception {
		run(env, peer);
		return true;
	}

	public abstract void run(JobContext env, Node origin) throws Exception;

}
