package edu.jlime.graphly.traversal.count;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.storenode.GraphlyCount;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class CountJob implements Job<GraphlyCount> {

	private Dir dir;
	private long[] data;
	int max_edges;
	private GraphlyGraph g;

	public CountJob(GraphlyGraph g, Dir dir, int max_edges, long[] array) {
		this.dir = dir;
		this.data = array;
		this.max_edges = max_edges;
		this.g = g;
	}

	@Override
	public GraphlyCount call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing count job for " + data.length);
		GraphlyCount l = g.countEdges(dir, max_edges, data);
		return l;
	}
}
