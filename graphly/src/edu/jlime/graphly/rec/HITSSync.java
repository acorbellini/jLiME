package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.salsa.SubGraphClean;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;

public class HITSSync implements RepeatSync<long[]> {

	private String auth;
	private String hub;

	public HITSSync(String auth, String hub) {
		this.auth = auth;
		this.hub = hub;
	}

	@Override
	public void exec(long[] before, Graphly g) throws Exception {
		g.getJobClient().getCluster().broadcast(new SubGraphClean("hits-sub"));
		g.commitUpdates(auth, hub);
	}

}
