package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;

public class WhoToFollowQC implements QueryContainer {

	@Override
	public void run(Graph g, long[] users, Mapper mapper) throws Exception {

		int steps = 200;
		if (users.length > 10)
			steps = 100;
		if (users.length > 30)
			steps = 50;

		g.v(users).setPrintSteps(true).set("mapper", mapper).as(Recommendation.class)
				.whotofollow("wtf-auth", "wtf-hub", steps, 5, 2000, steps, 5, 20)
				.submit(g.getJobClient().getCluster().getExecutors().get(0));
	}

	@Override
	public String getID() {
		return "whotofollow";
	}

}
