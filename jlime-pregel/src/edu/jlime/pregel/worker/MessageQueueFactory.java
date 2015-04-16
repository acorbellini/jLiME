package edu.jlime.pregel.worker;

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
}
