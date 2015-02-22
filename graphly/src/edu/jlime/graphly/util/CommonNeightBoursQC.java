package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;

public class CommonNeightBoursQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.commonNeighbours().exec();
	}

	@Override
	public String getID() {
		return "commonneighbours";
	}

}
