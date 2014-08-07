package edu.jlime.collections.adjacencygraph.query.local;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.query.MapProc;
import edu.jlime.collections.adjacencygraph.query.Query;

public class LocalMapQuery<T> implements Query<Map<Integer, T>> {

	private LocalListQuery q;

	private MapProc<T> mq;

	public LocalMapQuery(LocalListQuery q, MapProc<T> mq) {
		this.q = q;
		this.mq = mq;
	}

	@Override
	public Map<Integer, T> query() throws Exception {
		HashMap<Integer, T> map = new HashMap<Integer, T>();
		int[] u = q.query();
		for (int i : u) {
			map.putAll(mq.process(new LocalUserQuery(q.getStore(), i)));
		}
		return map;
	}
}
