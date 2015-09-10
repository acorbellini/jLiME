package edu.jlime.graphly.traversal.count;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.set.hash.TLongHashSet;

public class CountJob implements Job<GraphlyCount> {

	private Dir dir;
	int max_edges;
	private GraphlyGraph g;
	private long[] toFilter;
	private long[] keys;
	private float[] values;

	public CountJob(GraphlyGraph g, Dir dir, int max_edges, long[] keys,
			float[] values, long[] toFilter) {
		this.dir = dir;
		this.keys = keys;
		this.values = values;
		this.max_edges = max_edges;
		this.g = g;
		this.toFilter = toFilter;
	}

	@Override
	public GraphlyCount call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing count job for " + keys.length);
		return g.countEdges(dir, max_edges, keys, values, toFilter);
	}
}
