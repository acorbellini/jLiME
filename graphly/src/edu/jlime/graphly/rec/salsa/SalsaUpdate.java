package edu.jlime.graphly.rec.salsa;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.Update;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;

public class SalsaUpdate implements Update {

	private String a;
	private String h;
	private TLongArrayList authSet;
	private TLongArrayList hubSet;
	private long[] all;

	public SalsaUpdate(long[] all, TLongArrayList authSet,
			TLongArrayList hubSet, String authKey, String hubKey) {
		this.all = all;
		this.authSet = authSet;
		this.hubSet = hubSet;
		this.a = authKey;
		this.h = hubKey;
	}

	public Map<String, Object> exec(Long vid, Graphly g) throws Exception {

		SubGraph sg = g.getSubGraph("salsa-sub", all);

		sg.invalidateProperties();

		Map<String, Object> ret = new HashMap<>();
		float authCalc = 0f;
		long[] inEdges = sg.getEdges(Dir.IN, vid);
		for (long v : inEdges) {
			long[] outV = sg.getEdges(Dir.OUT, v);
			for (long w : outV) {
				int inW = sg.getEdgesCount(Dir.IN, w);
				if (inW > 0)
					authCalc += ((Float) sg.getProperty(w, a,
							1f / authSet.size()))
							/ (outV.length * inW);
			}
		}

		float hubCalc = 0f;
		long[] outEdges = sg.getEdges(Dir.OUT, vid);
		for (long v : outEdges) {
			long[] inV = sg.getEdges(Dir.IN, v);
			for (long w : inV) {
				int outW = sg.getEdgesCount(Dir.OUT, w);
				if (outW > 0)
					hubCalc += ((Float) sg
							.getProperty(w, h, 1f / hubSet.size()))
							/ (inV.length * outW);
			}
		}

		ret.put(a, authCalc);
		ret.put(h, hubCalc);
		return ret;
	}
}