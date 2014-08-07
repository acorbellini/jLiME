package edu.jlime.linkprediction.structural;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighboursSorensen implements ForEachQueryProc<Float> {

	private static ExecutorService exec = Executors.newCachedThreadPool();

	private static final long serialVersionUID = -4219606166158089226L;

	private ListQuery query;

	public SimilarityNeighboursSorensen(ListQuery neighbours) {
		this.query = neighbours;
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
		int[] neighbours = qFut.get();
		int[] otherNeighbours = nFut.get();
		int intersect = 2 * IntArrayUtils.intersectCount(neighbours,
				otherNeighbours);
		double root = otherNeighbours.length + neighbours.length;
		return intersect / (float) root;
	}

}
