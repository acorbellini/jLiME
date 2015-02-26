package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
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
		g.commitUpdates(auth, hub);
	}

}
