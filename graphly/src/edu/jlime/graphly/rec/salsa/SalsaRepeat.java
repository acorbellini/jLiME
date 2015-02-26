package edu.jlime.graphly.rec.salsa;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaRepeat implements Repeat<long[]> {
	private String authKey;
	private String hubKey;
	private TLongArrayList authSet;
	private TLongArrayList hubSet;
	private long[] all;

	public SalsaRepeat(String authKey, String hubKey, TLongArrayList authSet,
			TLongArrayList hubSet) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.authSet = authSet;
		this.hubSet = hubSet;
		TLongHashSet sub = new TLongHashSet(authSet);
		sub.addAll(hubSet);
		all = sub.toArray();
		Arrays.sort(all);
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {
		SubGraph sg = g.getSubGraph("salsa-sub", all);

		sg.invalidateProperties();

		HashMap<Long, Map<String, Object>> temps = new HashMap<>();

		for (long vid : before) {
			Map<String, Object> ret = salsa(sg, vid);
			temps.put(vid, ret);
		}

		g.setTempProperties(before, temps);

		return before;
	}

	private Map<String, Object> salsa(SubGraph sg, long vid) throws Exception {

		Map<String, Object> ret = new HashMap<>();
		float authCalc = 0f;
		long[] inEdges = sg.getEdges(Dir.IN, vid);
		for (long v : inEdges) {
			long[] outV = sg.getEdges(Dir.OUT, v);
			for (long w : outV) {
				int inW = sg.getEdgesCount(Dir.IN, w);
				if (inW > 0)
					authCalc += ((Float) sg.getProperty(w, authKey,
							1f / authSet.size())) / (outV.length * inW);
			}
		}

		float hubCalc = 0f;
		long[] outEdges = sg.getEdges(Dir.OUT, vid);
		for (long v : outEdges) {
			long[] inV = sg.getEdges(Dir.IN, v);
			for (long w : inV) {
				int outW = sg.getEdgesCount(Dir.OUT, w);
				if (outW > 0)
					hubCalc += ((Float) sg.getProperty(w, hubKey,
							1f / hubSet.size()))
							/ (inV.length * outW);
			}
		}

		ret.put(authKey, authCalc);
		ret.put(hubKey, hubCalc);
		return ret;
	}

}
