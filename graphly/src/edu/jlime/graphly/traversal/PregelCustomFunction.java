package edu.jlime.graphly.traversal;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;
import gnu.trove.decorator.TLongSetDecorator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class PregelCustomFunction implements CustomFunction {

	private VertexFunction func;
	private int steps;
	private boolean execOnAll;

	public PregelCustomFunction(VertexFunction func, int steps,
			boolean execOnAll) {
		this.func = func;
		this.steps = steps;
		this.execOnAll = execOnAll;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		TLongArrayList list = before.vertices();

		Mapper mapper = (Mapper) tr.get("mapper");

		Graphly g = tr.getGraph();

		TLongSetDecorator dec = new TLongSetDecorator(new TLongHashSet(list));

		PregelConfig conf = new PregelConfig().executeOnAll(execOnAll)
				.setvList(dec).steps(steps).threads(8)
				.graph(new GraphlyPregelAdapter(g))
				.split(new MapperPregelAdapter(mapper, tr.getGraph().getRpc()));
		PregelClient cli = tr.getGraph().getPregeClient();
		cli.execute(func, conf);
		return before;

	}
}
