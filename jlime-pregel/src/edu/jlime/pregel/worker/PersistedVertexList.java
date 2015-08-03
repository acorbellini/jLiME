package edu.jlime.pregel.worker;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TByteArrayList;

public class PersistedVertexList implements VertexList {
	public static class PersistedLongIterator implements LongIterator {
		private BufferedInputStream reader;
		long curr = -1;
		private boolean closed = false;

		byte[] asBytes = new byte[8];

		public PersistedLongIterator(PersistedVertexList list)
				throws FileNotFoundException {
			this.reader = new BufferedInputStream(
					new FileInputStream(list.file));

		}

		public boolean hasNext() throws IOException {

			if (!closed && reader.read(asBytes, 0, 8) == -1) {
				reader.close();
				closed = true;
				return false;
			}
			curr = DataTypeUtils.byteArrayToLong(asBytes);
			return true;
		}

		public long next() {
			return curr;
		}
	}

	private static final int MAX = 512 * 1024;

	// private TByteArrayList cache = new TByteArrayList();
	private File file;
	private BufferedOutputStream writer;
	int cont = 0;

	public PersistedVertexList() throws IOException {
		this.file = new File(System.getProperty("java.io.tmpdir") + "/"
				+ UUID.randomUUID() + ".list");
		this.file.createNewFile();
		this.writer = new BufferedOutputStream(new FileOutputStream(file), MAX);
	}

	public void add(long vid) throws IOException {
		// cache.add(DataTypeUtils.longToByteArray(vid));
		// if (cache.size() == MAX) {
		// this.flush();
		// System.out.println(cont);
		// }
		this.writer.write(DataTypeUtils.longToByteArray(vid));
		cont++;
	}

	public void flush() throws IOException {
		// this.writer.write(cache.toArray());
		// this.cache.clear();
	}

	public void close() throws IOException {
		this.flush();
		this.writer.close();
	}

	public void delete() {
		file.delete();
	}

	public PersistedLongIterator iterator() throws FileNotFoundException {
		return new PersistedLongIterator(this);
	}

	public int size() {
		return cont;
	}
}
