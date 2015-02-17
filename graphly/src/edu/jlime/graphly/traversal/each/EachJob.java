package edu.jlime.graphly.traversal.each;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class EachJob<T> extends RunJob {

	private long vid;
	private ForEach<T> fe;
	private int steps;
	private String k;
	private long[] vids;

	public EachJob(int steps, String key, long[] vids, ForEach<T> fe) {
		this.vids = vids;
		this.fe = fe;
		this.steps = steps;
		this.k = key;
	}

	@Override
	public void run(JobContext env, ClientNode origin) throws Exception {
		for (long vid : vids) {
			List<T> subres = new ArrayList<>();
			Graphly g = (Graphly) env.getGlobal("graphly");
			for (int i = 0; i < steps; i++) {
				T feRes = fe.exec(vid, g);
				subres.add(feRes);
			}
			g.setProperty(vid, k, subres);
		}
	}

}