package edu.jlime.util.table;

public class Sum extends Function {

	private Range r;

	public Sum(Range r) {
		this.r = r;
	}

	@Override
	public String calc() {
		Double sum = 0d;
		for (Cell c : r) {
			sum += Double.valueOf(c.value());
		}
		return sum.toString();
	}

}
