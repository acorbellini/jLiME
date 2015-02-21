package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

import java.util.HashMap;
import java.util.Map;

class HITSRepeat implements Repeat<long[]> {
	private final class HITSUpdate implements Update {
		private TLongHashSet originalList;
		private String a;
		private String h;

		public HITSUpdate(TLongHashSet current, String authKey, String hubKey) {
			this.originalList = current;
			this.a = authKey;
			this.h = hubKey;
		}

		public Map<String, Object> exec(Long vid, Graphly g) throws Exception {
			Map<String, Object> ret = new HashMap<>();
			float sumAuth = 0f;
			float sumAuthQuad = 0f;
			int contAuth = 0;
			TLongArrayList inedges = new TLongArrayList(g.getEdges(Dir.IN, vid));
			inedges.retainAll(new TLongHashSet(originalList));
			TLongIterator it = inedges.iterator();
			while (it.hasNext()) {
				long in = it.next();
				Float currHub = (Float) g.getProperty(in, h,
						(float) Math.sqrt(1f / originalList.size()));
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
						(float) Math.sqrt(1f / originalList.size()));
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
	private TLongHashSet current;

	public HITSRepeat(String authKey, String hubKey, long[] current) {
		this.authKey = authKey;
		this.hubKey = hubKey;
		this.current = new TLongHashSet(current);
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {
		return g.v(before).update(new HITSUpdate(current, authKey, hubKey))
				.exec();
	}
}