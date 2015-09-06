package edu.jlime.util.table;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

public class CSVBuilder {

	private File csv;

	private String columnSep = ";";

	private String decimalSep = ",";

	private int expectedFields = -1;

	boolean useWindowsNewLine = false;

	private int maxLines = -1;

	private HashMap<String, String> replaces = new HashMap<>();

	public void setUseWindowsNewLine(boolean useWindowsNewLine) {
		this.useWindowsNewLine = useWindowsNewLine;
	}

	public CSVBuilder(File csv) {
		this.csv = csv;
	}

	public CSVBuilder(String string) {
		this(new File(string));
	}

	public static String escape(String sep) {
		return sep.replaceAll("//.", "//.");
	}

	public static String[] split(String line, String sep) {
		return line.split("(?<!\\\\)" + sep, -1);
	}

	private static String[] compress(int expectedFields, String[] split,
			String sep) {
		String[] compressed = new String[expectedFields];
		for (int i = 0; i < split.length; i++) {
			if (i < expectedFields)
				compressed[i] = split[i];
			else
				compressed[expectedFields - 1] += sep + split[i];
		}
		return compressed;
	}

	public interface RowListener {

		public void onNewRow(String[] r);
	}

	public Table toTable() throws Exception {
		final Table table = new Table();
		try {
			read(new RowListener() {

				@Override
				public void onNewRow(String[] split) {
					Row row = table.newRow();
					for (int i = 0; i < split.length; i++) {
						ValueCell c = new ValueCell(split[i]);

						if (!isInt(c.value()))
							try {
								String newVal = c.value().replaceAll(
										escape(getDecimalSep()), ".");
								Double.valueOf(newVal);
								c.setValue(newVal);
								c.setFormat(Table.DoubleFormatter);
							} catch (Exception e) {
							}
						row.add(c);
					}
				}
			});
		} catch (FileNotFoundException e) {
		}
		return table;

	}

	protected boolean isInt(String value) {
		try {
			Integer.valueOf(value);
			return true;
		} catch (Exception e) {
		}
		return false;
	}

	public void read(RowListener rl) throws IOException {

		Logger log = Logger.getLogger(Table.class);

		CSVReader reader = new CSVReader(csv, useWindowsNewLine);

		int cont = 0;

		String sep = escape(getColumnSep());

		while (reader.ready() && (getMaxLines() < 0 || cont++ < getMaxLines())) {

			String line = reader.readLine();

			for (Entry<String, String> rep : replaces.entrySet())
				line = line.replaceAll(rep.getKey(), rep.getValue());

			String[] split = split(line, sep);

			if (getExpectedFields() > 0) {

				if (split.length > getExpectedFields()) {
					if (log.isDebugEnabled())
						log.debug("Line " + line + " had more fields ("
								+ split.length + ") than expected ("
								+ getExpectedFields()
								+ "). Merging into last line.");

					split = compress(getExpectedFields(), split, getColumnSep());
				} else if (split.length < getExpectedFields()) {
					// if (log.isDebugEnabled())
					// log.debug("Discarding line " + line +
					// " had less fields ("
					// + split.length + ") than expected ("
					// + expectedFields + ").");

					if (log.isDebugEnabled())
						log.debug("Joining line " + line + " had less fields ("
								+ split.length + ") than expected ("
								+ getExpectedFields() + ").");

					StringBuilder b = new StringBuilder(line);
					int size = split.length;
					while (size < getExpectedFields()) {
						if (!reader.ready())
							return;
						b.append(reader.readLine());
						line = b.toString();
						split = split(line, sep);
						size = split.length;
					}

					if (split.length > getExpectedFields())
						split = compress(getExpectedFields(), split,
								getColumnSep());
					if (log.isDebugEnabled())
						log.debug("Resulting line from joing is " + line
								+ " had fields (" + split.length + ").");

				}
			}

			rl.onNewRow(split);
		}
		reader.close();
	}

	public String getColumnSep() {
		return columnSep;
	}

	public void setColumnSep(String columnSep) {
		this.columnSep = columnSep;
	}

	public String getDecimalSep() {
		return decimalSep;
	}

	public void setDecimalSep(String decimalSep) {
		this.decimalSep = decimalSep;
	}

	public int getExpectedFields() {
		return expectedFields;
	}

	public void setExpectedFields(int expectedFields) {
		this.expectedFields = expectedFields;
	}

	public int getMaxLines() {
		return maxLines;
	}

	public void setMaxLines(int maxLines) {
		this.maxLines = maxLines;
	}

	public void setReplaceString(String from, String to) {
		replaces.put(from, to);

	}

	public int getCount() {
		try {
			return countLines(csv.getAbsolutePath());
		} catch (Exception e) {
		}
		return 0;
	}

	public static int countLines(String filename) throws IOException {
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
		try {
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			boolean empty = true;
			while ((readChars = is.read(c)) != -1) {
				empty = false;
				for (int i = 0; i < readChars; ++i) {
					if (c[i] == '\n') {
						++count;
					}
				}
			}
			return (count == 0 && !empty) ? 1 : count;
		} finally {
			is.close();
		}
	}
}
