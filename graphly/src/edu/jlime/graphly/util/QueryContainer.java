package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.Mapper;

public interface QueryContainer {
	public void run(Graph g, long[] users, Mapper mapper) throws Exception;

	public String getID();
}
