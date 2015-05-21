package edu.jlime.graphly.rec.hits;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;

public class HITSCustomStep implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;
	private int max_edges;

	public HITSCustomStep(String auth, String hub, int steps, int max_edges) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.max_edges = max_edges;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		Logger log = Logger.getLogger(HITSCustomStep.class);

		long[] subgraph = before.vertices().toArray();
		log.info("Executing HITS on " + subgraph.length);
		Arrays.sort(subgraph);
		GraphlyGraph g = tr.getGraph();
		return g.v(subgraph)
				.set("mapper", tr.get("mapper"))
				.repeat(steps, new HITSRepeat(auth, hub, subgraph),
						new HITSSync(auth, hub)).exec();
	}

	@Override
	public String toString() {
		return "HITSCustomStep [auth=" + auth + ", hub=" + hub + ", steps="
				+ steps + ", max_edges=" + max_edges + "]";
	}
}