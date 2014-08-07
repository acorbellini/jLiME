package edu.jlime.linkprediction.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import edu.jlime.util.table.Cell;
import edu.jlime.util.table.Function;
import edu.jlime.util.table.FunctionCell;
import edu.jlime.util.table.Functions;
import edu.jlime.util.table.Functions.CellFactory;
import edu.jlime.util.table.Range;
import edu.jlime.util.table.Table;

public class ResultsGrouping {

	private static final String SEP = ";";

	public static void main(String[] args) throws Exception {
		File dir = new File(args[0]);
		List<File> files = findAll(dir, new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(".csv") && name.contains("run");
			}
		});

		for (File file : files) {
			fix(file);
		}

	}

	private static void fix(File file) throws Exception {
		File tmp = new File(file.getPath() + ".tmp");

		Table t = Table.readCSV(file, ",", ".");

		t.sortTableHeader(new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				Integer i1 = Integer.valueOf(o1.replaceAll("GridCluster", ""));
				Integer i2 = Integer.valueOf(o2.replaceAll("GridCluster", ""));
				return i1.compareTo(i2);
			}
		});

		int rowsWithData = t.getRowLimit() - 1;
		int colsWithData = t.getColLimit() - 1;
		t.fillCol("Sum", 1, rowsWithData, 1, colsWithData, Functions.SUM);

		t.fillRow("Max", 1, colsWithData + 1, 1, rowsWithData, Functions.MAX);
		t.fillRow("Diff", 1, colsWithData + 1, 1, rowsWithData,
				new CellFactory() {

					@Override
					public Cell build(final Range r) {
						return new FunctionCell(new Function() {
							@Override
							public String calc() {
								Double f = Double.valueOf(r.first());
								Double l = Double.valueOf(r.last());
								return new Double(l - f).toString();
							}
						});
					}
				});

		BufferedWriter writer = new BufferedWriter(new FileWriter(tmp));
		writer.write(t.print(";"));
		writer.close();

		file.delete();
		tmp.renameTo(file);
	}

	private static List<File> findAll(File dir, FilenameFilter filenameFilter) {
		File[] files = dir.listFiles(filenameFilter);
		ArrayList<File> found = new ArrayList<>();
		for (File file : files) {
			found.add(file);
		}
		for (File file : dir.listFiles()) {
			if (file.isDirectory())
				found.addAll(findAll(file, filenameFilter));
		}

		return found;
	}
}
