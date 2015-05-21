package edu.jlime.graphly.rec.hits;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.SubGraphClean;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;

public class HITSSync implements RepeatSync<long[]> {

	private String auth;
	private String hub;

	public HITSSync(String auth, String hub) {
		this.auth = auth;
		this.hub = hub;
	}

	@Override
	public void exec(long[] before, GraphlyGraph g) throws Exception {
		g.getJobClient().getCluster()
				.broadcast(new SubGraphClean(g, "hits-sub"));
		g.commitUpdates(auth, hub);
	}

}
