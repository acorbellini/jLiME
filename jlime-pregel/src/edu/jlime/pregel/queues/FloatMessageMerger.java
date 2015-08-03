package edu.jlime.pregel.queues;

import edu.jlime.pregel.mergers.MessageMerger;

public interface FloatMessageMerger extends MessageMerger {

	public float merge(float to, float msg2);
}
