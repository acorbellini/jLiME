package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;

public class HITSQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		int steps = 100;
		if (users.length > 10)
			steps = 50;
		if (users.length > 30)
			steps = 25;
		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.hits("hits-auth", "hits-hub", steps, 200).exec();
	}

	@Override
	public String getID() {
		return "hits";
	}
}
