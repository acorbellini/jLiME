package edu.jlime.util;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class CSV implements Closeable {

	private boolean exists = true;

	private String sep = ",";

	private Writer writer;

	public CSV(Writer writer) {
		this.writer = writer;
	}

	public CSV(Writer writer, String... header) throws IOException {
		this(writer);
		putLine(header);
	}

	public boolean alreadyExisted() {
		return exists;
	}

	StringBuffer line = new StringBuffer();

	public void put(String el) {
		line.append(sep + el);
	}

	public void newLine() {
		String row = line.substring(1) + "\n";
		try {
			writer.append(row);
		} catch (IOException e) {
			e.printStackTrace();
		}
		line = new StringBuffer();
	}

	public void putLine(String... col) {
		for (String s : col)
			put(s);
		newLine();
	}

	public void setSep(String sep) {
		this.sep = sep;
	}

	public static CSV fileCSV(String file, boolean append, String... header) throws IOException {
		File f = new File(file);
		File parentDir = f.getParentFile();
		parentDir.mkdirs();
		if (header.length > 0 && !f.exists())
			return new CSV(new FileWriter(f, append), header);
		else
			return new CSV(new FileWriter(f, append));
	}

	@Override
	public void close() throws IOException {
		writer.close();
	}
}
