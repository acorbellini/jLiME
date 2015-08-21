package edu.jlime.graphly.rec.hits;

public class DivideUpdateProperty implements FloatPropertyUpdate {

	private float div;

	public DivideUpdateProperty(float sum_hub) {
		this.div = sum_hub;
	}

	@Override
	public float update(float curr) {
		return curr / div;
	}

}
