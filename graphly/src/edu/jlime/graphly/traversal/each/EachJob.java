package edu.jlime.graphly.traversal.each;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.graphly.client.Graph;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class EachJob<T> extends RunJob {

	private long vid;
	private ForEach<T> fe;
	private int steps;
	private String k;
	private long[] vids;
	private Graph g;

	public EachJob(Graph g, int steps, String key, long[] vids, ForEach<T> fe) {
		this.vids = vids;
		this.fe = fe;
		this.steps = steps;
		this.k = key;
		this.g = g;
	}

	@Override
	public void run(JobContext env, Node origin) throws Exception {
		for (long vid : vids) {
			List<T> subres = new ArrayList<>();
			for (int i = 0; i < steps; i++) {
				T feRes = fe.exec(vid, g);
				if (feRes != null)
					subres.add(feRes);
			}
			g.setProperty(vid, k, subres);
		}
	}

}