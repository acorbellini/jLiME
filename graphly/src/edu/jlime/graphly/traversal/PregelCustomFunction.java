package edu.jlime.graphly.traversal;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;
import gnu.trove.set.hash.TLongHashSet;

public class PregelCustomFunction implements CustomFunction {

	private VertexFunction func;
	private PregelConfig config;

	public PregelCustomFunction(VertexFunction func, PregelConfig config) {
		this.func = func;
		this.config = config;
	}

	@Override
	public PregelResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		TLongHashSet list = before.vertices();

		Mapper mapper = (Mapper) tr.get("mapper");

		GraphlyGraph g = tr.getGraph();

		PregelConfig conf = config.setPureBSP(true)
				.graph(GraphlyPregelAdapter.getFactory(g.getGraph()))
				.split(new MapperPregelAdapter(mapper, tr.getGraph().getRpc()));
		PregelClient cli = tr.getGraph().getPregeClient();
		PregelExecution res = cli.execute(func, list.toArray(), conf);
		return new PregelResult(before.vertices(), res);

	}

	public PregelConfig getConfig() {
		return config;
	}
}
