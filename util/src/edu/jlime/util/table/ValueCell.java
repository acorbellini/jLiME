package edu.jlime.util.table;

public class ValueCell extends Cell {

	String v;

	public ValueCell(String v) {
		this.v = v;
	}

	@Override
	public String value() {
		return v;
	}

	public void setValue(String v) {
		this.v = v;
	}
}
