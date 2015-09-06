package edu.jlime.util.table;

public class Average extends Function {

	private Range range;

	public Average(Range range) {
		this.range = range;
	}

	@Override
	public String calc() {
		double sum = 0;
		double count = 0;
		for (Cell c : range) {
			if (c == null) {
				return "ERROR: CELL IS NOT DEFINED";
			}
			sum += Double.valueOf(c.value());
			count++;
		}
		return new Double(sum / count).toString();
	}

}
