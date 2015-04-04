package edu.jlime.graphly.rec.salsa;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.list.array.TLongArrayList;

public class SalsaStep implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;

	public SalsaStep(String auth, String hub, int steps) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		Logger log = Logger.getLogger(SalsaStep.class);

		long[] subgraph = before.vertices().toArray();
		Arrays.sort(subgraph);
		log.info("Executing Salsa Step on " + subgraph.length);

		GraphlyGraph g = tr.getGraph();

		log.info("Filtering authority side");
		TLongArrayList authSet = g.v(subgraph)
				.filter(new MinEdgeFilter(Dir.IN, 1, subgraph)).exec()
				.vertices();
		log.info("Filtering hub side");
		TLongArrayList hubSet = g.v(subgraph)
				.filter(new MinEdgeFilter(Dir.OUT, 1, subgraph)).exec()
				.vertices();
		log.info("Executing SalsaRepeat with hubset " + hubSet.size()
				+ " and auth " + authSet.size());
		return g.v(subgraph)
				.set("mapper", tr.get("mapper"))
				.repeat(steps, new SalsaRepeat(auth, hub, authSet, hubSet),
						new SalsaSync(auth, hub)).exec();
	}
}