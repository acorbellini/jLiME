package edu.jlime.graphly.blueprints;

import java.util.ArrayList;
import java.util.List;

import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class GraphlyStep<E extends Element> extends GraphStep<E> {
	public final List<HasContainer> hasContainers = new ArrayList<>();

	public GraphlyStep(GraphStep<E> originalGraphStep) {
		super(originalGraphStep.getTraversal(), originalGraphStep
				.getGraph(TinkerGraph.class), originalGraphStep
				.getReturnClass(), originalGraphStep.getIds());

	}

}
