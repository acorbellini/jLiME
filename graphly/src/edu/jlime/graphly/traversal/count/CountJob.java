package edu.jlime.graphly.traversal.count;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.RemoteReference;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.map.hash.TLongFloatHashMap;

public class CountJob implements Job<RemoteReference<GraphlyCount>> {

	private Dir dir;
	private TLongFloatHashMap data;
	int max_edges;
	private GraphlyGraph g;

	public CountJob(GraphlyGraph g, Dir dir, int max_edges,
			TLongFloatHashMap prevCounts) {
		this.dir = dir;
		this.data = prevCounts;
		this.max_edges = max_edges;
		this.g = g;
	}

	@Override
	public RemoteReference<GraphlyCount> call(JobContext ctx, ClientNode peer)
			throws Exception {
		Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing count job for " + data.size());
		GraphlyCount l = g.countEdges(dir, max_edges, data);
		return new RemoteReference<GraphlyCount>(l, ctx, true);
	}
}
