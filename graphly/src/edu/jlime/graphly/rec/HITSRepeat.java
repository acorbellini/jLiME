package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;

class HITSRepeat implements Repeat<long[]> {
	private String authKey;
	private String hubKey;
	private long[] current;

	public HITSRepeat(String authKey, String hubKey, long[] current) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.current = current;
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {

		SubGraph sg = g.getSubGraph("hits-sub", current);

		sg.invalidateProperties();

		return g.v(before).update(new HITSUpdate(current, authKey, hubKey))
				.exec();
	}
}