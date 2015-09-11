package edu.jlime.graphly.traversal.count;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class CountJob implements Job<Count> {

	private Dir dir;
	int max_edges;
	private Graph g;
	private long[] toFilter;
	private long[] keys;
	private float[] values;

	public CountJob(Graph g, Dir dir, int max_edges, long[] keys, float[] values, long[] toFilter) {
		this.dir = dir;
		this.keys = keys;
		this.values = values;
		this.max_edges = max_edges;
		this.g = g;
		this.toFilter = toFilter;
	}

	@Override
	public Count call(JobContext ctx, Node peer) throws Exception {
		Logger log = Logger.getLogger(CountJob.class);
		log.info("Executing count job for " + keys.length);
		return g.countEdges(dir, max_edges, keys, values, toFilter);
	}
}
