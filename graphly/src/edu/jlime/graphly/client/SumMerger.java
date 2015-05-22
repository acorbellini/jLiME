package edu.jlime.graphly.client;

import java.util.List;

public class SumMerger implements GatherMerger<Float> {

	@Override
	public Float merge(List<Float> merge) {
		float sum = 0f;
		for (Float float1 : merge) {
			sum += float1;
		}
		return sum;
	}

}
