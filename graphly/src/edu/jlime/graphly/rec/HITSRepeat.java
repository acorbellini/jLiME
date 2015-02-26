package edu.jlime.graphly.rec;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.traversal.Dir;

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

		HashMap<Long, Map<String, Object>> temps = new HashMap<>();

		for (long vid : before) {
			Map<String, Object> ret = hitsforvid(sg, vid);
			temps.put(vid, ret);
		}

		g.setTempProperties(before, temps);

		return before;
	}

	private Map<String, Object> hitsforvid(SubGraph sg, long vid)
			throws ExecutionException {
		Map<String, Object> ret = new HashMap<>();
		float sumAuth = 0f;
		float sumAuthQuad = 0f;
		int contAuth = 0;
		for (long in : sg.getEdges(Dir.IN, vid)) {
			final long curr = in;

			Float currHub = (Float) sg.getProperty(curr, hubKey,
					(float) Math.sqrt(1f / current.length));
			float quad = currHub * currHub;
			sumAuth += currHub;
			sumAuthQuad += quad;
			contAuth++;
		}

		float sumHub = 0f;
		float sumHubQuad = 0f;
		int contHub = 0;
		for (long out : sg.getEdges(Dir.OUT, vid)) {
			final long curr = out;

			Float currAuth = (Float) sg.getProperty(curr, authKey,
					(float) Math.sqrt(1f / current.length));
			float quad = currAuth * currAuth;
			sumHub += currAuth;
			sumHubQuad += quad;
			contHub++;
		}
		ret.put(authKey,
				contAuth == 0 ? 0 : sumAuth / (float) Math.sqrt(sumAuthQuad));
		ret.put(hubKey,
				contHub == 0 ? 0 : sumHub / (float) Math.sqrt(sumHubQuad));
		return ret;
	}
}