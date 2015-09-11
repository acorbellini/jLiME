package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;

public class SALSAQC implements QueryContainer {

	@Override
	public void run(Graph g, long[] users, Mapper mapper) throws Exception {
		int steps = 100;
		if (users.length > 10)
			steps = 50;
		if (users.length > 30)
			steps = 25;

		g.v(users).set("mapper", mapper).to(Dir.BOTH, 500).as(Recommendation.class)
				.salsaHybrid("salsa-auth", "salsa-hub", steps, 10)
				.submit(g.getJobClient().getCluster().getExecutors().get(0));
	}

	@Override
	public String getID() {
		return "salsa";
	}
}
