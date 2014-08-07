package edu.jlime.util.table;

public class Promedio extends Function {

	private Range range;

	public Promedio(Range range) {
		this.range = range;
	}

	@Override
	public String calc() {
		double sum = 0;
		double count = 0;
		for (Cell c : range) {
			sum += Double.valueOf(c.value());
			count++;
		}
		return new Double(sum / count).toString();
	}

}
