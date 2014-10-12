package edu.jlime.util.table;

public class StdDev extends Function {

	private Range r;

	public StdDev(Range r) {
		this.r = r;
	}

	@Override
	public String calc() {
		double sum = 0d;
		double sum2 = 0d;
		int count = 0;
		for (Cell c : r) {
			if(c==null)
				return "CELL IS NOT DEFINED";
			Double v = Double.valueOf(c.value());
			sum += v;
			sum2 += v * v;
			count++;
		}
		double avg = sum / count;
		return new Double(Math.sqrt(sum2 / count - avg * avg)).toString();
	}

}
