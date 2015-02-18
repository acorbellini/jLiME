package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.jd.client.JobContext;
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
		this.query = query;
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

		final TIntIntHashMap countres = query.exec(c);
		Logger logger = Logger.getLogger(TopQuery.class);
		logger.info("Obtaining " + top + " elements from query.");
		TreeSet<Integer> finalRes = new TreeSet<Integer>(
				new Comparator<Integer>() {

					@Override
					public int compare(Integer o1, Integer o2) {
						int comp = Integer.compare(countres.get(o1),
								countres.get(o2));
						if (comp == 0)
							return o1.compareTo(o2);
						return comp;
					}
				});
		TIntIntIterator it = countres.iterator();
		while (it.hasNext()) {
			it.advance();
			int k = it.key();
			int v = it.value();
			if (finalRes.size() < top)
				finalRes.add(k);
			else {
				if (v > countres.get(finalRes.first())) {
					finalRes.remove(finalRes.first());
					finalRes.add(k);
				}
			}
		}

		logger.info("Finished obtaining " + top + " elements from query.");
		List<int[]> res = new ArrayList<>();
		for (Integer k : finalRes) {
			res.add(new int[] { k, countres.get(k) });
		}
		Collections.sort(res, new Comparator<int[]>() {

			@Override
			public int compare(int[] o1, int[] o2) {
				return new Integer(o1[1]).compareTo(new Integer(o2[1])) * -1;
			}

		});
		return res;
	}
}
