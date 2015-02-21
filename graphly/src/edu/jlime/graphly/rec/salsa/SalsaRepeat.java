package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.Repeat;
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
		// Arrays.sort(all);
	}

	@Override
	public Object exec(long[] before, Graphly g) throws Exception {
		return g.v(before)
				.update(new SalsaUpdate(all, authSet, hubSet, authKey, hubKey))
				.exec();
	}

}
