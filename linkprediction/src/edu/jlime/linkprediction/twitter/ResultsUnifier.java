package edu.jlime.linkprediction.twitter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;

import edu.jlime.util.table.FunctionCell;
import edu.jlime.util.table.Functions;
import edu.jlime.util.table.Average;
import edu.jlime.util.table.Row;
import edu.jlime.util.table.Table;
import edu.jlime.util.table.ValueCell;

public class ResultsUnifier {

	private static final int NUM_RUNS = 1;
	private static final int NUM_NODES = 8;

	public static void main(String[] args) throws Exception {
		File dir = new File(args[0]);

		FilenameFilter filterMem = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains("-mem-");
			}
		};
		FilenameFilter filterNet = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.contains("-net-");
			}
		};

		unify(dir, filterMem, "mem");
		unify(dir, filterNet, "net");

		unifyTimes(dir);
	}

	private static void unifyTimes(File dir) throws Exception {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				dir.getPath() + "/avgtimes.csv")));
		Table times = new Table();
		int[] users = getUsers(dir);
		boolean header = true;
		for (int u : users) {
			if (header) {
				Row r = times.newRow();
				r.add(new ValueCell("User"));
				for (File strategy : dir.listFiles()) {
					if (strategy.isDirectory()) {
						r.add(new ValueCell(strategy.getName()));
					}
				}
				header = false;
			}

			Row r = times.newRow();
			r.add(new ValueCell(u + ""));
			for (File strategy : dir.listFiles()) {
				if (strategy.isDirectory()) {
					Table timeTable = Table.readCSV(new File(dir.getPath()
							+ "/" + strategy.getName() + "/" + u + "/" + u
							+ "-" + strategy.getName() + ".csv"), ",", ".",
							false);
					timeTable.set(0, timeTable.getRowLimit(), new ValueCell(
							"Avg"));
					timeTable.set(
							3,
							timeTable.getRowLimit() - 1,
							new FunctionCell(new Average(timeTable.colRange(3,
									1, timeTable.getRowLimit() - 2))));
					r.add(timeTable.getRow(timeTable.getRowLimit() - 1).get(3));
				}
			}

		}

		times.fillRow("Avg", 1, times.getColLimit() - 1, 1,
				times.getRowLimit() - 1, Functions.AVERAGE);

		times.fillRow("StdDev", 1, times.getColLimit() - 1, 1,
				times.getRowLimit() - 1, Functions.STDDEV);

		writer.write(times.print(";"));
		writer.close();
	}

	private static void unify(File dir, FilenameFilter filterMem, String title)
			throws FileNotFoundException, Exception, IOException {
		Table generalMax = new Table();
		Table generalDiff = new Table();

		BufferedWriter writerMax = new BufferedWriter(new FileWriter(new File(
				dir.getPath() + "/max-" + title + ".csv")));
		BufferedWriter writerDiff = new BufferedWriter(new FileWriter(new File(
				dir.getPath() + "/diff-" + title + ".csv")));

		boolean header = true;

		int[] users = getUsers(dir);
		for (int u : users) {
			if (header) {
				Row r = generalMax.newRow();
				Row rdiff = generalDiff.newRow();
				r.add(new ValueCell("User"));
				rdiff.add(new ValueCell("User"));
				for (File strategy : dir.listFiles()) {
					if (strategy.isDirectory()) {
						r.add(new ValueCell(strategy.getName()));
						rdiff.add(new ValueCell(strategy.getName()));
					}
				}
				header = false;
			}
			Row userMaxRow = generalMax.newRow();
			userMaxRow.add(new ValueCell(u + ""));

			Row userDiffRow = generalDiff.newRow();
			userDiffRow.add(new ValueCell(u + ""));
			for (File strategy : dir.listFiles()) {
				if (strategy.isDirectory()) {

					Table tmax = new Table();
					Table tdiff = new Table();
					boolean first = true;
					File strategyDir = new File(dir.getPath() + "/"
							+ strategy.getName() + "/" + u);
					File[] listFiles = strategyDir.listFiles(filterMem);
					for (File file : listFiles) {

						Table run = Table.readCSV(file, ",", ".", false);
						if (first) {
							Row r = tmax.newRow();
							r.add(run.getRow(0));
							Row r2 = tdiff.newRow();
							r2.add(run.getRow(0));
							first = false;
						}

						Row max = run.getRow("Max");
						Row diff = run.getRow("Diff");

						Row row = tmax.newRow();
						row.add(max);
						row = tdiff.newRow();
						row.add(diff);
					}

					tmax.fillRow("Avg", 1, tmax.getColLimit() - 1, 1,
							tmax.getRowLimit() - 1, Functions.AVERAGE);
					tmax.fillRow("StdDev", 1, tmax.getColLimit() - 1, 1,
							tmax.getRowLimit() - 1, Functions.STDDEV);

					tdiff.fillRow("Avg", 1, tdiff.getColLimit() - 1, 1,
							tdiff.getRowLimit() - 1, Functions.AVERAGE);
					tdiff.fillRow("StdDev", 1, tdiff.getColLimit() - 1, 1,
							tdiff.getRowLimit() - 1, Functions.STDDEV);

					userMaxRow
							.add(tmax.getRow(NUM_RUNS + 1).get(NUM_NODES + 1));
					userDiffRow.add(tdiff.getRow(NUM_RUNS + 1).get(
							NUM_NODES + 1));
				}
			}
		}

		generalMax.fillRow("Avg", 1, generalMax.getColLimit() - 1, 1,
				generalMax.getRowLimit() - 1, Functions.AVERAGE);

		generalMax.fillRow("StdDev", 1, generalMax.getColLimit() - 1, 1,
				generalMax.getRowLimit() - 1, Functions.STDDEV);

		generalDiff.fillRow("Avg", 1, generalDiff.getColLimit() - 1, 1,
				generalDiff.getRowLimit() - 1, Functions.AVERAGE);

		generalDiff.fillRow("StdDev", 1, generalDiff.getColLimit() - 1, 1,
				generalDiff.getRowLimit() - 1, Functions.STDDEV);

		writerMax.write(generalMax.print(";"));
		writerMax.close();
		writerDiff.write(generalDiff.print(";"));
		writerDiff.close();
	}

	private static int[] getUsers(File dir) {
		File strat = null;
		while (strat == null) {
			for (File f : dir.listFiles()) {
				if (f.isDirectory()) {
					strat = f;
					continue;
				}
			}
		}

		File[] listFiles = strat.listFiles();
		int[] users = new int[listFiles.length];
		int cont = 0;
		for (File f : listFiles) {
			users[cont++] = Integer.valueOf(f.getName());
		}

		return users;
	}
}
