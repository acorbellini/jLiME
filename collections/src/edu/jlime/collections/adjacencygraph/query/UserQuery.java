package edu.jlime.collections.adjacencygraph.query;

import java.util.Arrays;

import edu.jlime.client.JobContext;
import edu.jlime.collections.adjacencygraph.RemoteAdjacencyGraph;

public class UserQuery extends RemoteListQuery {

	public UserQuery(RemoteAdjacencyGraph graph, int[] data) {
		super(graph);
		this.data = data;
	}

	private static final long serialVersionUID = -8046739284248347712L;

	private int[] data;

	// public UserQuery(int[] data, AdjacencyGraph graph) {
	// super(graph);
	// this.data = data;
	// }

	@Override
	public int[] doExec(JobContext c) throws Exception {
		return data;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof UserQuery))
			return false;
		UserQuery other = (UserQuery) obj;
		return Arrays.equals(data, other.getData());
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(data);
	}

	public int[] getData() {
		return data;
	}
}
