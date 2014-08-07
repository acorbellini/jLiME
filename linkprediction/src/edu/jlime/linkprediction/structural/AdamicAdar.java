package edu.jlime.linkprediction.structural;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.adjacencygraph.query.RemoteListQuery;

public class AdamicAdar implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = 1679024761693885271L;

	private RemoteListQuery query;

	public AdamicAdar(RemoteListQuery queryToCompareWith) {
		this.query = queryToCompareWith;
	}

	@Override
	public Float call(ListQuery userId) throws Exception {
		Map<Integer, Float> map = userId.neighbours().intersect(query)
				.foreach(new CalcLog()).query();
		float adamicVal = 0;
		for (Float v : map.values()) {
			adamicVal += v;
		}
		return adamicVal;
	}
}