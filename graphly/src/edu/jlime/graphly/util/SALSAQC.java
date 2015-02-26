package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;

public class SALSAQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		int steps = 500;
		if (users.length > 10)
			steps = 250;
		if (users.length > 30)
			steps = 100;

		g.v(users).set("mapper", mapper).to(Dir.BOTH, 50).as(Recommendation.class)
				.salsa("salsa-auth", "salsa-hub", steps).exec();
	}

	@Override
	public String getID() {
		return "salsa";
	}
}
