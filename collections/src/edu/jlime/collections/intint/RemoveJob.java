package edu.jlime.collections.intint;

import edu.jlime.collections.hash.SimpleIntIntHash;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class RemoveJob implements Job<Boolean> {

	private static final long serialVersionUID = 2394174940738419243L;

	private int[] toRemove;

	private String internal;

	public RemoveJob(int[] toRemove, String internalHashName) {
		this.toRemove = toRemove;
		this.internal = internalHashName;
	}

	@Override
	public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
		SimpleIntIntHash intHash = (SimpleIntIntHash) ctx.get(internal);
		for (int i : toRemove) {
			intHash.remove(i);
		}
		return true;
	}

}
