package edu.jlime.jd;

import edu.jlime.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class SetEnvironment extends RunJob {

	private static final long serialVersionUID = -8419850780391279407L;

	private String k;

	private Object v;

	public SetEnvironment(String k, Object v) {
		this.k = k;
		this.v = v;
	}

	@Override
	public void run(JobContext env, ClientNode origin) throws Exception {
		// System.out.println("Setting object on "
		// + env.getCluster().getLocalPeer());
		env.put(k, v);
		// System.out.println("Returning, object set.");
	}

}
