package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.SubGraphClean;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;

public class SalsaSync implements RepeatSync<long[]> {

	private String auth;
	private String hub;

	public SalsaSync(String auth, String hub) {
		this.auth = auth;
		this.hub = hub;
	}

	@Override
	public void exec(long[] before, Graph g) throws Exception {
		g.getJobClient().getCluster().broadcast(new SubGraphClean(g, "salsa-sub"));
		g.commitUpdates(auth, hub);
	}

}
