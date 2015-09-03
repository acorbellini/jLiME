package edu.jlime.graphly.rec;

import java.util.concurrent.atomic.AtomicInteger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.pregel.worker.LongIterator;

public class GraphCache {

	private GraphlyGraph g;
	private int max;

	private LongIterator it;

	AtomicInteger current = new AtomicInteger(0);

	public GraphCache(long[] data, int from, int to, GraphlyGraph g, int max) {
		this.it = new ArrayRangeLongIterator(data, from, to);
		this.g = g;
		this.max = max;
	}

}
