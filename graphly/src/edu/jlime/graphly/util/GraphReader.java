package edu.jlime.graphly.util;

import gnu.trove.list.array.TLongArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

final class GraphReader implements Iterator<Pair<Long, long[]>> {
	private final BufferedReader reader;
	private String nextLine;
	private String sep;

	GraphReader(String file, String sep) throws FileNotFoundException {
		this.reader = new BufferedReader(new FileReader(new File(file)));
		this.sep = sep;
	}

	@Override
	public Pair<Long, long[]> next() {
		TLongArrayList map = new TLongArrayList();
		Long curr = null;
		boolean done = false;
		try {
			while ((nextLine != null || reader.ready()) && !done) {

				String line = "";
				if (nextLine != null) {
					line = nextLine;
					nextLine = null;
				} else
					line = reader.readLine();

				int indexOf = line.indexOf(sep);
				if (indexOf < 0)
					System.out.println("Broken line " + line + " sep: " + sep);
				long k = Long.valueOf(line.substring(0, indexOf));

				if (curr == null)
					curr = k;

				if (curr != k) {
					nextLine = line;
					done = true;
				} else {
					long v = Long.valueOf(line.substring(indexOf + 1,
							line.length()));
					map.add(v);
				}
			}
			if (!reader.ready())
				this.reader.close();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return Pair.build(curr, map.toArray());
	}

	@Override
	public boolean hasNext() {
		try {
			return nextLine != null || reader.ready();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}
}