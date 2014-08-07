package edu.jlime.util.table;

public abstract class Cell {

	public interface Formatter {

		public String format(Cell c);
	}

	Formatter format = new Formatter() {
		public String format(Cell c) {
			return c.value();
		};
	};

	public void setFormat(Formatter format) {
		this.format = format;
	}

	public abstract String value();

	@Override
	public String toString() {
		return format.format(this);
	}
}
