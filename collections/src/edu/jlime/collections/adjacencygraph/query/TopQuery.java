package edu.jlime.collections.adjacencygraph.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.Mapper;
import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DataTypeUtils;
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

		final byte[] countres = query.exec(c);
		ByteBuffer reader = new ByteBuffer(countres);
		int[] keys = DataTypeUtils.byteArrayToIntArray(reader.getByteArray());
		final int[] values = DataTypeUtils.byteArrayToIntArray(reader
				.getByteArray());
		Integer[] order = new Integer[keys.length];
		for (int i = 0; i < order.length; i++) {
			order[i] = i;
		}
		System.out.println("Sorting result");
		Arrays.sort(order, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				if (values[o1] == values[o2])
					return 0;
				else if (values[o1] < values[o2])
					return 1;
				else
					return -1;

			}
		});
		System.out.println("Keeping best " + top);
		List<int[]> res = new ArrayList<>();
		for (int i = 0; i < top; i++) {
			res.add(new int[] { keys[order[i]], values[order[i]] });
		}

		System.out.println("Finished top");
		return res;
		// Logger logger = Logger.getLogger(TopQuery.class);
		// logger.info("Obtaining " + top + " elements from query.");
		// TIntIntHashMap finalRes = new TIntIntHashMap();
		// TIntIntIterator it = countres.iterator();
		// while (it.hasNext()) {
		// it.advance();
		// int k = it.key();
		// int v = it.value();
		// if (finalRes.size() < top)
		// finalRes.put(k, v);
		// else {
		// int minK = 0;
		// int minVal = Integer.MAX_VALUE;
		// for (TIntIntIterator iterator = finalRes.iterator(); iterator
		// .hasNext();) {
		// iterator.advance();
		// if (minVal > iterator.value()) {
		// minK = iterator.key();
		// minVal = iterator.value();
		// }
		// }
		// if (minVal < v) {
		// finalRes.remove(minK);
		// finalRes.put(k, v);
		// }
		// }
		// it.remove();
		// }
		//
		// logger.info("Finished obtaining " + top + " elements from query.");
		// List<int[]> res = new ArrayList<>();
		// TIntIntIterator finalIt = finalRes.iterator();
		// while (finalIt.hasNext()) {
		// finalIt.advance();
		// res.add(new int[] { finalIt.key(), finalIt.value() });
		// }
		// Collections.sort(res, new Comparator<int[]>() {
		//
		// @Override
		// public int compare(int[] o1, int[] o2) {
		// return new Integer(o1[1]).compareTo(new Integer(o2[1])) * -1;
		// }
		//
		// });
		//
		// // if (delete)
		// // countres.delete(c.getCluster());
		//
		// return res;
	}
}
