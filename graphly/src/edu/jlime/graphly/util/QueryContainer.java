package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;

public interface QueryContainer {
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception;

	public String getID();
}
