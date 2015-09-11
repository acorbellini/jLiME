package edu.jlime.pregel.queues;

import edu.jlime.pregel.mergers.MessageMergers.FloatArrayMerger;
import edu.jlime.pregel.mergers.ObjectMessageMerger;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;

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

	public static MessageQueueFactory floatQueue(final FloatTroveMessageMerger merger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new FloatMessageQueueImpl(merger);
			}
		};
	}

	public static MessageQueueFactory doubleQueue(final DoubleMessageMerger doubleMessageMerger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				DoubleMessageQueue ret = new DoubleMessageQueue(doubleMessageMerger);
				return ret;
			}
		};
	}

	public static MessageQueueFactory floatArrayFactory(final FloatArrayMerger messageMerger) {
		return new MessageQueueFactory() {

			@Override
			public PregelMessageQueue getMQ() {
				return new FloatArrayMessageQueue(messageMerger);
			}
		};
	}

	// public static MessageQueueFactory floatMapDBQueue(
	// final FloatMapDBMerger merger) {
	// return new MessageQueueFactory() {
	//
	// @Override
	// public PregelMessageQueue getMQ() {
	// return new FloatMapDBQueue(merger);
	// }
	// };
	// }

	// public static MessageQueueFactory floatBigTextFormatQueue(
	// final FloatMapDBMerger merger) {
	// return new MessageQueueFactory() {
	//
	// @Override
	// public PregelMessageQueue getMQ() {
	// return new BigTextFormatQueue(merger);
	// }
	// };
	// }
}
