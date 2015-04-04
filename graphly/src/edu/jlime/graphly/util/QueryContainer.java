package edu.jlime.graphly.util;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;

public interface QueryContainer {
	public void run(GraphlyGraph g, long[] users, Mapper mapper)
			throws Exception;

	public String getID();
}
