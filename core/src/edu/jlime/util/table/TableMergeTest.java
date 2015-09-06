package edu.jlime.util.table;

import java.io.File;

public class TableMergeTest {

	public static void main(String[] args) throws Exception {
		CSVBuilder builder = new CSVBuilder(new File("tableMerge.txt"));

		Table t = builder.toTable();

		while (t.getRowLimit() > 2)
			t.mergeRows(1, 2);

		System.out.println(t);
	}
}
