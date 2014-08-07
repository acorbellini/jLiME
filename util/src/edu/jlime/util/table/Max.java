package edu.jlime.util.table;

public class Max extends Function {

	private Range r;

	public Max(Range r) {
		this.r = r;
	}

	@Override
	public String calc() {
		Double max = 0d;
		for (Cell c : r) {
			Double v = Double.valueOf(c.value());
			if (v > max)
				max = v;
		}
		return max.toString();
	}

}
