package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;

public abstract class GraphlyRun {

	private String name;

	public GraphlyRun(String name) {
		this.setName(name);
	}

	abstract Traversal run(long[] users, Graph graph, Mapper mapper) throws Exception;

	abstract String printResult(TraversalResult res, Graph g) throws Exception;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
