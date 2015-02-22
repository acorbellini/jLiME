package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;

public class ExploratoryCountQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.exploratoryCount(3000, 10, Dir.OUT, Dir.IN, Dir.OUT).exec();
	}

	@Override
	public String getID() {
		return "exploratory";
	}

}
