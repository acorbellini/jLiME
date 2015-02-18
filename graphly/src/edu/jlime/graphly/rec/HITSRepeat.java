package edu.jlime.graphly.rec;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

class HITSRepeat implements Repeat<long[]> {
	private final class HITSUpdate implements Update {
		private long[] originalList;
		private String a;
		private String h;

		public HITSUpdate(long[] before, String authKey, String hubKey) {
			this.originalList = before;
			this.a = authKey;
			this.h = hubKey;
		}

		public Map<String, Object> exec(Long vid, Graphly g) throws Exception {
			Map<String, Object> ret = new HashMap<>();
			float sumAuth = 0f;
			float sumAuthQuad = 0f;
			int contAuth = 0;
			TLongArrayList inedges = new TLongArrayList(g.getEdges(Dir.IN, vid));
			inedges.retainAll(originalList);
			TLongIterator it = inedges.iterator();
			while (it.hasNext()) {
				long in = it.next();
				Float currHub = (Float) g.getProperty(in, h,
						(float) Math.sqrt(1f / originalList.length));
				sumAuth += currHub;
				sumAuthQuad += currHub * currHub;
				contAuth++;
			}

			float sumHub = 0f;
			int contHub = 0;
			float sumHubQuad = 0f;
			TLongArrayList outedges = new TLongArrayList(g.getEdges(Dir.OUT,
					vid));
			outedges.retainAll(originalList);
			TLongIterator itOut = outedges.iterator();
			while (itOut.hasNext()) {
				long out = itOut.next();
				Float currAuth = (Float) g.getProperty(out, a,
						(float) Math.sqrt(1f / originalList.length));
				sumHub += currAuth;
				sumHubQuad += currAuth * currAuth;
				contHub++;
			}
			// ret.put(a, sumAuth);
			ret.put(a,
					contAuth == 0 ? 0 : sumAuth
							/ (float) Math.sqrt(sumAuthQuad));
			// ret.put(h, sumHub);
			ret.put(h,
					contHub == 0 ? 0 : sumHub / (float) Math.sqrt(sumHubQuad));

			// System.out.println(vid + ":" + ret.get(a) + " -- " + ret.get(h));
			return ret;
		}
	}

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
		return g.v(before).update(new HITSUpdate(current, authKey, hubKey))
				.exec();
	}
}