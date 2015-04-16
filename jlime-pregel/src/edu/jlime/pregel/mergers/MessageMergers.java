package edu.jlime.pregel.mergers;

import edu.jlime.pregel.worker.FloatMessageMerger;

public class MessageMergers {
	public static FloatMessageMerger FLOAT_SUM = new FloatMessageMerger() {

		@Override
		public float merge(float msg1, float msg2) {
			return msg1 + msg2;
		}

	};
	// public static class SumMerger implements MessageMerger {
	//
	// @Override
	// public void merge(PregelMessage v1, PregelMessage v2, PregelMessage into)
	// {
	// into.setV((Double) v1.getV() + (Double) v2.getV());
	// }
	//
	// }
	//
	// public static MessageMerger sum() {
	// return new SumMerger();
	// }

}
