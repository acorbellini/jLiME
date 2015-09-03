package edu.jlime.graphly.util;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;

public abstract class GraphlyRun {

	private String name;

	public GraphlyRun(String name) {
		this.setName(name);
	}

	abstract GraphlyTraversal run(long[] users, GraphlyGraph graph,
			Mapper mapper) throws Exception;

	abstract String printResult(TraversalResult res, GraphlyGraph g)
			throws Exception;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
