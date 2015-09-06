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
	private TLongFloatMap data;
	int max_edges;
	private GraphlyGraph g;
	private TLongHashSet toFilter;

	public CountJob(GraphlyGraph g, Dir dir, int max_edges,
			TLongFloatMap prevCounts, TLongHashSet toFilter2) {
		this.dir = dir;
		this.data = prevCounts;
		this.max_edges = max_edges;
		this.g = g;
		this.toFilter = toFilter2;
	}

	@Override
	public GraphlyCount call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing count job for " + data.size());
		return g.countEdges(dir, max_edges, data, toFilter);
	}
}
