package edu.jlime.graphly.rec.salsa;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.graphly.rec.Repeat;
import edu.jlime.graphly.rec.Update;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaRepeat implements Repeat<long[]> {
	private final class SalsaUpdate implements Update {
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

			Map<String, Object> ret = new HashMap<>();
			float authCalc = 0f;
			long[] inEdges = sg.getEdges(Dir.IN, vid);
			for (long v : inEdges) {
				// if (authSet.contains(v) || hubSet.contains(v)) {
				long[] outV = sg.getEdges(Dir.OUT, v);
				for (long w : outV)
				// if (authSet.contains(w) || hubSet.contains(w))
				{
					int inW = sg.getEdgesCount(Dir.IN, w);
					if (inW > 0)
						authCalc += ((Float) g.getProperty(w, a,
								1f / authSet.size()))
								/ (outV.length * inW);
				}
			}

			float hubCalc = 0f;
			long[] outEdges = sg.getEdges(Dir.OUT, vid);
			for (long v : outEdges)
			// if (authSet.contains(v) || hubSet.contains(v))
			{
				long[] inV = sg.getEdges(Dir.IN, v);
				for (long w : inV)
				// if (authSet.contains(w) || hubSet.contains(w))
				{
					int outW = sg.getEdgesCount(Dir.OUT, w);
					if (outW > 0)
						hubCalc += ((Float) g.getProperty(w, h,
								1f / hubSet.size()))
								/ (inV.length * outW);
				}
			}

			ret.put(a, authCalc);
			ret.put(h, hubCalc);
			return ret;
		}
	}

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
		// Arrays.sort(all);
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {
		return g.v(before)
				.update(new SalsaUpdate(all, authSet, hubSet, authKey, hubKey))
				.exec();
	}

}
