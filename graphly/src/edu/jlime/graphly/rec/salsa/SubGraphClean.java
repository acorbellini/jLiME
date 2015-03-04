package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.SubGraph;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class SubGraphClean extends RunJob {

	private String key;

	public SubGraphClean(String k) {
		this.key = k;
	}

	@Override
	public void run(JobContext env, ClientNode origin) throws Exception {
		Graphly g = (Graphly) env.getGlobal("graphly");
		SubGraph sg = g.getSubGraph(key);
		sg.invalidateProperties();
		sg.invalidateTemps();
	}
}
