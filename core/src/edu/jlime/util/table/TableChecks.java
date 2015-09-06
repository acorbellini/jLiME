package edu.jlime.util.table;

public class TableChecks {

	public interface TableCheck {

		boolean check(String val);
	}

	public static final TableCheck IntegerCheck = new TableCheck() {

		@Override
		public boolean check(String val) {
			try {
				Integer.valueOf(val);
				return true;
			} catch (NumberFormatException e) {
				// e.printStackTrace();
			}
			return false;
		}
	};

}
