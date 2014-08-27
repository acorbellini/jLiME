package edu.jlime.util;

import java.lang.ref.WeakReference;
import java.util.HashMap;

public class ByteArrayCache {

	private static final int listSize = 8;

	private static HashMap<Integer, WeakReference<byte[]>[]> usable = new HashMap<>();

	private static int max = 128;

	private static int min = 32;

	public static void put(byte[] notUsed) {
		int size = notUsed.length;
		if (size < min || size > max)
			return;
		WeakReference<byte[]>[] list = usable.get(size);
		if (list == null)
			synchronized (usable) {
				list = usable.get(size);
				if (list == null) {
					list = new WeakReference[listSize];
					usable.put(size, list);
				}
			}
		synchronized (list) {
			for (int i = 0; i < list.length; i++) {
				WeakReference<byte[]> weak = list[i];
				if (weak == null || weak.get() == null) {
					list[i] = new WeakReference<byte[]>(notUsed);
					return;
				}
			}
		}

	}

	public static byte[] get(int size) {
		try {
			WeakReference<byte[]>[] list = usable.get(size);
			synchronized (list) {
				for (int i = 0; i < list.length; i++) {
					WeakReference<byte[]> weak = list[i];
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