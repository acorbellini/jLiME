package edu.jlime.collections.arrayutils;

import edu.jlime.collections.util.IntArrayUtils;
import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;

import org.junit.Test;

public class IntersectTest {

	@Test
	public void intersect() throws Exception {
		TIntArrayList array = new TIntArrayList();
		Scanner scanner = new Scanner(new File("miarray.txt"));
		while (scanner.hasNext()) {
			array.add(scanner.nextInt());
		}
		scanner.close();
		int[] asarray = array.toArray();
		System.out.println(asarray.length);
		Arrays.sort(asarray);
		int[] a = new int[] { 1, 1, 1, 1, 23, 41, 3, 23, 2, 51, 6576, 2247, 10,
				34, 23, 8, 9, 7, 5, 1, 6, 10000 };

		int[] b = new int[] { 1, 1, 6, 7, 90, 3, 5, 4, 7, 8, 41, 12, 5, 9, 01,
				45, 5, 1, 10000, 1, 1 };

		int[] c = IntArrayUtils.intersectArrays(a, b);

		System.out.println(Arrays.toString(c));

		int[] a1 = new int[(int) (Math.random() * 1000000)];
		int[] b1 = new int[(int) (Math.random() * 1000000)];
		for (int i = 0; i < a1.length; i++) {
			a1[i] = (int) (Math.random() * 1000000);

		}
		for (int i = 0; i < b1.length; i++)
			b1[i] = (int) (Math.random() * 1000000);

		int[] c1 = IntArrayUtils.intersectArrays(a1, b1);
		//
		System.out.println(Arrays.toString(c1));

	}
}
