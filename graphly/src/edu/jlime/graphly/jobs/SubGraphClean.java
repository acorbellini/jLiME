package edu.jlime.graphly.jobs;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class SubGraphClean extends RunJob {

	private String key;
	private Graph g;

	public SubGraphClean(Graph g, String k) {
		this.key = k;
		this.g = g;
	}

	@Override
	public void run(JobContext env, Node origin) throws Exception {
		SubGraph sg = g.getSubGraph(key);
		sg.invalidateProperties();
		sg.invalidateTemps();
	}
}
