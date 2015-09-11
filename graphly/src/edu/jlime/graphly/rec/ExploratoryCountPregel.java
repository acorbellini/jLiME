package edu.jlime.graphly.rec;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.mergers.MessageMergers;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;

public class ExploratoryCountPregel implements CustomFunction {

	private int top;

	public ExploratoryCountPregel(int top) {
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {

		PregelConfig config = PregelConfig.create().merger("ec", MessageMergers.floatSum()).steps(4);

		Graph g = tr.getGraph();

		g.v(before.vertices()).set("mapper", tr.get("mapper")).as(Pregel.class)
				.vertexFunction(new ExploratoryCountVertexFunction(), config).exec();

		Logger log = Logger.getLogger(ExploratoryCountPregel.class);

		long init = System.currentTimeMillis();

		Set<Pair<Long, Float>> set = g.topFloat("ec", top);

		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			ret.put(pair.left, pair.right);
		}
		log.info("Finished obtaining top in " + (System.currentTimeMillis() - init) + " ms");
		return new CountResult(ret);
	}
}
