package edu.jlime.graphly.blueprints;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

public class GraphlyGraphStepStrategy extends AbstractTraversalStrategy {

	private static final GraphlyGraphStepStrategy INSTANCE = new GraphlyGraphStepStrategy();

	private GraphlyGraphStepStrategy() {
	}

	@Override
	public void apply(final Traversal.Admin<?, ?> traversal,
			final TraversalEngine engine) {
		if (engine.equals(TraversalEngine.COMPUTER))
			return;

		final Step<?, ?> startStep = TraversalHelper.getStart(traversal);
		if (startStep instanceof GraphStep) {
			final GraphStep<?> originalGraphStep = (GraphStep) startStep;
			final GraphlyStep<?> tinkerGraphStep = new GraphlyStep<>(
					originalGraphStep);
			TraversalHelper.replaceStep(startStep, (Step) tinkerGraphStep,
					traversal);

			Step<?, ?> currentStep = tinkerGraphStep.getNextStep();
			while (true) {
				if (currentStep instanceof HasContainerHolder) {
					tinkerGraphStep.hasContainers
							.addAll(((HasContainerHolder) currentStep)
									.getHasContainers());
					if (currentStep.getLabel().isPresent()) {
						final IdentityStep identityStep = new IdentityStep<>(
								traversal);
						identityStep.setLabel(currentStep.getLabel().get());
						TraversalHelper.insertAfterStep(identityStep,
								currentStep, traversal);
					}
					traversal.removeStep(currentStep);
				} else if (currentStep instanceof IdentityStep) {
					// do nothing
				} else {
					break;
				}
				currentStep = currentStep.getNextStep();
			}
		}
	}

	public static GraphlyGraphStepStrategy instance() {
		return INSTANCE;
	}
}
