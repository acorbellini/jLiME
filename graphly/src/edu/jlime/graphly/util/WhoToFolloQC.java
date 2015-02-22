package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;

public class WhoToFolloQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.whotofollow("wtf-auth", "wtf-hub", 1000, 10, 100, 1000, 5, 20).exec();
	}

	@Override
	public String getID() {
		return "whotofollow";
	}

}
