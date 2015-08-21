package edu.jlime.graphly.util;

import edu.jlime.pregel.aggregators.FloatDifferenceAggregator;

public class MessageAggregators {

	public static FloatSumAggregator floatSum() {
		return new FloatSumAggregator();
	}

	public static FloatDifferenceAggregator floatDiff() {
		return new FloatDifferenceAggregator();
	}

}
