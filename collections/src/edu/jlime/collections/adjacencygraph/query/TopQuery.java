package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

public class TopQuery extends RemoteQuery<List<int[]>> {

	private static final long serialVersionUID = -1411479222536631191L;

	private RemoteCountQuery query;

	private int top;

	private boolean delete;

	public TopQuery(RemoteCountQuery query, int top) {
		this(query, top, true);
	}

	public TopQuery(RemoteCountQuery query, int top, boolean delete) {
		super(query.getGraph());
		this.top = top;
		this.delete = delete;
	}

	@Override
	public String getMapName() {
		return query.getMapName();
	}

	@Override
	public Mapper getMapper() {
		return query.getMapper();
	}

	@Override
	public List<int[]> doExec(JobContext c) throws Exception {
		Logger.getLogger(TopQuery.class).info(
				"Obtaining " + top + " elements from query.");
		TIntIntHashMap finalRes = new TIntIntHashMap();

		final TIntIntHashMap countres = query.exec(c);
		TIntIntIterator it = countres.iterator();
		while (it.hasNext()) {
			it.advance();
			int k = it.key();
			int v = it.value();
			if (finalRes.size() < top)
				finalRes.put(k, v);
			else {
				int minK = 0;
				int minVal = Integer.MAX_VALUE;
				for (TIntIntIterator iterator = finalRes.iterator(); iterator
						.hasNext();) {
					iterator.advance();
					if (minVal > iterator.value()) {
						minK = iterator.key();
						minVal = iterator.value();
					}
				}
				if (minVal < v) {
					finalRes.remove(minK);
					finalRes.put(k, v);
				}
			}
			it.remove();
		}
		List<int[]> res = new ArrayList<>();
		TIntIntIterator finalIt = finalRes.iterator();
		while (finalIt.hasNext()) {
			finalIt.advance();
			res.add(new int[] { finalIt.key(), finalIt.value() });
		}
		Collections.sort(res, new Comparator<int[]>() {

			@Override
			public int compare(int[] o1, int[] o2) {
				return new Integer(o1[1]).compareTo(new Integer(o2[1])) * -1;
			}

		});

		// if (delete)
		// countres.delete(c.getCluster());

		return res;
	}
}
