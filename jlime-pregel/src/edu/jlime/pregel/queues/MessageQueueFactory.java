package edu.jlime.pregel.queues;

import edu.jlime.pregel.mergers.MessageMergers.FloatArrayMerger;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
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
				return new FloatMessageQueue(merger);
			}
		};
	}

	public static MessageQueueFactory doubleQueue(
			final DoubleMessageMerger doubleMessageMerger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				DoubleMessageQueue ret = new DoubleMessageQueue(
						doubleMessageMerger);
				return ret;
			}
		};
	}

	public static MessageQueueFactory floatArrayFactory(
			final FloatArrayMerger messageMerger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new FloatArrayMessageQueue(messageMerger);
			}
		};
	}
}
