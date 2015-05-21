package edu.jlime.graphly.jobs;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class SubGraphClean extends RunJob {

	private String key;
	private GraphlyGraph g;

	public SubGraphClean(GraphlyGraph g, String k) {
		this.key = k;
		this.g = g;
	}

	@Override
	public void run(JobContext env, ClientNode origin) throws Exception {
		SubGraph sg = g.getSubGraph(key);
		sg.invalidateProperties();
		sg.invalidateTemps();
	}
}
