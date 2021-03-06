package edu.jlime.graphly.traversal;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class VertexJob implements Job<long[]> {

	private static final long serialVersionUID = -3316802861448545540L;

	private long[] data;

	private Dir dir;

	private int max_edges;

	private Graph g;

	public VertexJob(Graph g, Dir dir, int max_edges, long[] data) {
		this.dir = dir;
		this.data = data;
		this.max_edges = max_edges;
		this.g = g;
	}

	@Override
	public long[] call(JobContext ctx, Node peer) throws Exception {
		Logger log = Logger.getLogger(VertexJob.class);
		if (log.isDebugEnabled())
			log.debug("Vertex Job obtaining " + data.length + ".");
		return g.getEdgesMax(dir, max_edges, data);
	}
}