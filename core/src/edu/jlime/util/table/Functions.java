package edu.jlime.util.table;

public class Functions {

	public interface CellFactory {

		Cell build(Range r);
	}

	public static CellFactory AVERAGE = new CellFactory() {

		@Override
		public Cell build(Range r) {
			return new FunctionCell(new Average(r));
		}
	};

	public static CellFactory SUM = new CellFactory() {

		@Override
		public Cell build(Range r) {
			return new FunctionCell(new Sum(r));
		}
	};

	public static CellFactory STDDEV = new CellFactory() {

		@Override
		public Cell build(Range r) {
			return new FunctionCell(new StdDev(r));
		}
	};

	public static CellFactory MAX = new CellFactory() {

		@Override
		public Cell build(Range r) {
			return new FunctionCell(new Max(r));
		}
	};

}
