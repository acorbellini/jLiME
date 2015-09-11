package edu.jlime.graphly.rec.hits;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.SubGraphClean;
import edu.jlime.graphly.traversal.RepeatStep.RepeatSync;
import gnu.trove.set.hash.TLongHashSet;

public class HITSSync implements RepeatSync<long[]> {

	private String auth;
	private String hub;

	public HITSSync(String auth, String hub) {
		this.auth = auth;
		this.hub = hub;
	}

	@Override
	public void exec(long[] before, Graph g) throws Exception {
		g.getJobClient().getCluster().broadcast(new SubGraphClean(g, "hits-sub"));
		g.commitFloatUpdates(auth, hub);
		TLongHashSet set = new TLongHashSet(before);
		float sum_hub = g.sumFloat(hub, set);
		float sum_auth = g.sumFloat(auth, set);
		g.updateFloatProperty(hub, new DivideUpdateProperty(sum_hub));
		g.updateFloatProperty(auth, new DivideUpdateProperty(sum_auth));

	}
}
