package edu.jlime.linkprediction.structural;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighbours implements ForEachQueryProc<Float> {

	private static ExecutorService exec = Executors.newCachedThreadPool();

	private static final long serialVersionUID = -5792006253005274592L;

	private ListQuery query;

	public SimilarityNeighbours(ListQuery queryToCompareWith) {
		this.query = queryToCompareWith;
	}

	@Override
	public Float call(final ListQuery userId) throws Exception {
		Future<int[]> qFut = exec.submit(new Callable<int[]>() {

			@Override
			public int[] call() throws Exception {
				return query.query();
			}
		});
		Future<int[]> nFut = exec.submit(new Callable<int[]>() {

			@Override
			public int[] call() throws Exception {
				return userId.neighbours().query();
			}
		});
		int[] followees = qFut.get();
		int[] otherNeighbours = nFut.get();
		int intersect = IntArrayUtils
				.intersectCount(followees, otherNeighbours);
		float union = (float) IntArrayUtils.unionCount(followees,
				otherNeighbours);
		return intersect / union;
	}
}