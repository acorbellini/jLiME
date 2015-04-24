package edu.jlime.pregel.queues;

import edu.jlime.pregel.messages.ObjectMessageMerger;
import edu.jlime.pregel.worker.FloatMessageMerger;

public abstract class MessageQueueFactory {
	public abstract PregelMessageQueue getMQ();

	public static MessageQueueFactory hashed(final ObjectMessageMerger merger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new HashedMessageQueue(merger);
			}
		};
	}

	public static MessageQueueFactory simple(final ObjectMessageMerger merger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new MessageQueue(merger);
			}
		};
	}

	public static MessageQueueFactory floatQueue(final FloatMessageMerger merger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new FloatHashedMessageQueue(merger);
			}
		};
	}

	public static MessageQueueFactory doubleQueue(
			final DoubleMessageMerger doubleMessageMerger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				DoubleHashedMessageQueue ret = new DoubleHashedMessageQueue(
						doubleMessageMerger);
				return ret;
			}
		};
	}
}
