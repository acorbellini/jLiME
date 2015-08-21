package edu.jlime.pregel.mergers;

import edu.jlime.pregel.queues.DoubleMessageMerger;
import edu.jlime.pregel.queues.FloatArrayMessageQueue;
import edu.jlime.pregel.queues.MessageQueueFactory;
import edu.jlime.pregel.worker.FloatTroveMessageMerger;
import gnu.trove.map.hash.TLongFloatHashMap;

public class MessageMergers {
	// public static final class FloatMapDBMerger implements MessageMerger {
	// public float merge(float msg1, float msg2) {
	// return msg1 + msg2;
	// }
	//
	// @Override
	// public MessageQueueFactory getFactory() {
	// return MessageQueueFactory.floatMapDBQueue(this);
	// }
	// }

	public static final class FloatArrayMerger implements MessageMerger {
		@Override
		public MessageQueueFactory getFactory() {
			return MessageQueueFactory.floatArrayFactory(this);
		}

		public void merge(float[] a, float[] b, float[] to) {
			for (int i = 0; i < a.length; i++) {
				if (a[i] == FloatArrayMessageQueue.NULL)
					to[i] = b[i];
				else if (b[i] == FloatArrayMessageQueue.NULL)
					to[i] = a[i];
				else
					to[i] = a[i] + b[i];
			}
		}
	}

	public static final MessageMerger FLOAT_ARRAY_SUM = new FloatArrayMerger();

	public static FloatTroveMessageMerger floatSum() {
		return new FloatTroveMessageMerger() {

			@Override
			public void merge(long to, float msg2, TLongFloatHashMap map) {
				map.adjustOrPutValue(to, msg2, msg2);
			}

		};
	}

	public static DoubleMessageMerger DOUBLE_SUM = new DoubleMessageMerger() {

		@Override
		public double merge(double msg1, double msg2) {
			return msg1 + msg2;
		}

	};

}
