package edu.jlime.graphly.util;

import edu.jlime.pregel.aggregators.FloatDifferenceAggregator;

public class MessageAggregators {

	public static FloatSumAggregator FLOAT_SUM = new FloatSumAggregator();
	public static FloatDifferenceAggregator FLOAT_DIFF = new FloatDifferenceAggregator();

}
