package edu.jlime.collections.util;

import gnu.trove.iterator.TIntIntIterator;
import gnu.trove.map.hash.TIntIntHashMap;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class TwitterTools {

	public static int[] setToArray(HashSet<Integer> target) {
		int[] arr = new int[target.size()];
		int cont = 0;
		Iterator<Integer> it = target.iterator();
		while (it.hasNext()) {
			Integer el = it.next();
			arr[cont] = el;
			cont++;
			it.remove();
		}
		return arr;
	}

	public static int[][] mapToArray(TIntIntHashMap target) {
		int[][] ret = new int[target.size()][2];
		int cont = 0;
		for (TIntIntIterator it = target.iterator(); it.hasNext();) {
			it.advance();
			ret[cont] = new int[] { it.key(), it.value() };
			cont++;
			it.remove();
		}
		return ret;
	}

	public static int[] listToArray(List<Integer> list) {
		int[] res = new int[list.size()];
		int cont = 0;
		Iterator<Integer> it = list.iterator();
		while (it.hasNext()) {
			Integer i = it.next();
			res[cont++] = i.intValue();
			it.remove();
		}
		return res;

	}

	public static int[][] doubleListToArray(List<Integer[]> list) {
		int[][] ret = new int[list.size()][2];
		int cont = 0;
		for (Integer[] integers : list) {
			ret[cont][0] = integers[0].intValue();
			ret[cont][1] = integers[1].intValue();
			cont++;
		}
		return ret;
	}

}