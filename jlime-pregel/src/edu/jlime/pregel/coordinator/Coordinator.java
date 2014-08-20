package edu.jlime.pregel.coordinator;

import java.util.List;

import edu.jlime.pregel.worker.VertexFunction;

public interface Coordinator {
	public void finished(int worker) throws Exception;

	public void execute(List<Integer> vertex, VertexFunction func)
			throws Exception;
}
