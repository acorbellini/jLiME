package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;

public class SALSAQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		int steps = 500;
		if (users.length > 10)
			steps = 250;
		if (users.length > 30)
			steps = 100;

		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.salsa("salsa-auth", "salsa-hub", steps, 5).exec();
	}

	@Override
	public String getID() {
		return "salsa";
	}
}
