package edu.jlime.pregel.mergers;

import edu.jlime.pregel.worker.MessageMerger;

public class MessageMergers {

	public static class SumMerger implements MessageMerger {

		@Override
		public Object merge(Object v1, Object v2) {
			return (Double) v1 + (Double) v2;
		}

	}

	public static MessageMerger sum() {
		return new SumMerger();
	}

}
