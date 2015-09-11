package edu.jlime.graphly.store;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;

import com.google.common.primitives.UnsignedBytes;

import edu.jlime.util.Pair;

public class RangeIterator implements Iterator<Pair<byte[], byte[]>> {
	Pair<byte[], byte[]> cursor = null;
	byte[] curr;
	byte[] to;
	int cont = 0;
	DBIterator iterator;
	boolean first = true;
	private int max;
	private boolean includeFirst;
	private DB db;

	public RangeIterator(boolean inclFirst, byte[] from, byte[] to, int max, DB db) {
		this.includeFirst = inclFirst;
		this.curr = from;
		this.to = to;
		this.iterator = db.iterator();
		this.max = max;
		this.db = db;
		boolean done = false;
		while (!done)
			try {
				iterator.seek(curr);
				done = true;
			} catch (Exception e) {
				if (!e.getMessage().contains("code: 32"))
					throw e;
				try {
					iterator.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				iterator = db.iterator();
			}
	}

	@Override
	public boolean hasNext() {
		boolean done = false;
		while (!done)
			try {
				while (iterator.hasNext() && !done) {
					if (cont > max)
						done = true;
					else {
						Entry<byte[], byte[]> e = iterator.next();
						byte[] key = e.getKey();
						curr = key;
						if (UnsignedBytes.lexicographicalComparator().compare(to, key) > 0) {
							if (!first || (first && includeFirst)) {
								first = false;
								cursor = new Pair<>(key, e.getValue());
								cont++;
								return true;
							}
							first = false;
						} else
							done = true;
					}
				}
			} catch (Exception e1) {
				if (!e1.getMessage().contains("code: 32"))
					return false;
				try {
					iterator.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				iterator = db.iterator();
				iterator.seek(curr);
				includeFirst = cont == 0 && includeFirst;
				first = true;
			}
		try {
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public Pair<byte[], byte[]> next() {
		return cursor;
	}

}
