package edu.jlime.util;

import java.lang.ref.SoftReference;
import java.util.HashMap;

public class ByteArrayCache {

	private static final int listSize = 64;

	private static HashMap<Integer, SoftReference<byte[]>[]> usable = new HashMap<>();

	private static int max = 512;

	private static int min = 32;

	public static void put(byte[] notUsed) {
		int size = notUsed.length;
		if (size < min || size > max)
			return;
		SoftReference<byte[]>[] list = usable.get(size);
		if (list == null)
			synchronized (usable) {
				list = usable.get(size);
				if (list == null) {
					list = new SoftReference[listSize];
					usable.put(size, list);
				}
			}
		synchronized (list) {
			for (int i = 0; i < list.length; i++) {
				SoftReference<byte[]> weak = list[i];
				if (weak == null || weak.get() == null) {
					list[i] = new SoftReference<byte[]>(notUsed);
					return;
				}
			}
		}

	}

	public static byte[] get(int size) {
		try {
			SoftReference<byte[]>[] list = usable.get(size);
			synchronized (list) {
				for (int i = 0; i < list.length; i++) {
					SoftReference<byte[]> weak = list[i];
					if (weak != null) {
						byte[] el = weak.get();
						if (el != null) {
							list[i] = null;
							return el;
						}
					}

				}
			}
		} catch (Exception e) {
		}
		return new byte[size];
	}
}