package edu.jlime.graphly.rec;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

final class HITSUpdate implements Update {
	private long[] originalList;
	private String a;
	private String h;

	public HITSUpdate(long[] current, String authKey, String hubKey) {
		this.originalList = current;
		this.a = authKey;
		this.h = hubKey;
	}

	public Map<String, Object> exec(Long vid, Graphly g) throws Exception {

		SubGraph sg = g.getSubGraph("hits-sub", originalList);

		Map<String, Object> ret = new HashMap<>();
		float sumAuth = 0f;
		float sumAuthQuad = 0f;
		int contAuth = 0;
		for (long in : sg.getEdges(Dir.IN, vid)) {
			Float currHub = (Float) g.getProperty(in, h,
					(float) Math.sqrt(1f / originalList.length));
			sumAuth += currHub;
			sumAuthQuad += currHub * currHub;
			contAuth++;
		}

		float sumHub = 0f;
		int contHub = 0;
		float sumHubQuad = 0f;
		for (long out : sg.getEdges(Dir.OUT, vid)) {
			Float currAuth = (Float) g.getProperty(out, a,
					(float) Math.sqrt(1f / originalList.length));
			sumHub += currAuth;
			sumHubQuad += currAuth * currAuth;
			contHub++;
		}
		// ret.put(a, sumAuth);
		ret.put(a, contAuth == 0 ? 0 : sumAuth / (float) Math.sqrt(sumAuthQuad));
		// ret.put(h, sumHub);
		ret.put(h, contHub == 0 ? 0 : sumHub / (float) Math.sqrt(sumHubQuad));

		// System.out.println(vid + ":" + ret.get(a) + " -- " + ret.get(h));
		return ret;
	}
}