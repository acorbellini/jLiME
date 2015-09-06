package edu.jlime.util.table;

public class FunctionCell extends Cell {

	private Function f;

	public FunctionCell(Function f) {
		this.f = f;
		setFormat(Table.DoubleFormatter);
	}

	@Override
	public String value() {
		return f.calc();
	}

}
