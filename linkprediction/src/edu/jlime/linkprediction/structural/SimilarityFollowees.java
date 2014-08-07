package edu.jlime.linkprediction.structural;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityFollowees implements ForEachQueryProc<Float> {

	private static ExecutorService exec = Executors.newCachedThreadPool();

	private static final long serialVersionUID = -630432249326453179L;

	private ListQuery query;

	public SimilarityFollowees(ListQuery queryToCompareWith) {
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
				return userId.followees().query();
			}
		});
		int[] followees = qFut.get();
		int[] otherFollowees = nFut.get();
		int intersect = IntArrayUtils.intersectCount(followees, otherFollowees);
		float union = (float) IntArrayUtils.unionCount(followees,
				otherFollowees);
		return intersect / union;
	}
}