package edu.jlime.util.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

public class CSVReader {

	BufferedReader reader;

	private boolean win;

	public CSVReader(File csv, boolean windows) throws FileNotFoundException {
		this(new FileReader(csv), windows);
	}

	public CSVReader(Reader r, boolean windows) {
		this(r, windows, 512 * 1024);

	}

	public CSVReader(Reader r, boolean windows, int size) {
		this.reader = new BufferedReader(r, size);
		this.win = windows;
	}

	public boolean ready() throws IOException {
		return reader.ready();
	}

	String peeked = null;

	public String readLine() {
		if (peeked != null) {
			String toret = peeked;
			peeked = null;
			return toret;
		}
		StringBuilder builder = new StringBuilder(1000);
		char last = ' ';
		try {
			while (ready()) {
				char curr = (char) reader.read();
				if ((!win || last == '\r') && curr == '\n')
					break;
				else
					last = curr;

				if (curr != '\r' && curr != '\n')
					builder.append(curr);
			}
		} catch (Exception e) {
			System.out.println(e);
		}

		return builder.toString();
	}

	public void close() throws IOException {
		reader.close();
	}

	public String peekLine() throws IOException {
		if (!ready())
			return null;

		if (peeked != null)
			return peeked;

		peeked = readLine();
		return peeked;
	}

}
