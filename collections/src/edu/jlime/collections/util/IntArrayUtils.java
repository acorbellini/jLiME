package edu.jlime.collections.util;

import gnu.trove.set.hash.TIntHashSet;

import java.util.Arrays;

public class IntArrayUtils {

	public static int[] intersectArrays(int[] A, int[] B) {
		TIntHashSet intersect = new TIntHashSet();
		int i = 0;
		int j = 0;
		while (i < A.length && j < B.length) {
			int a = A[i];
			int b = B[j];
			if (a == b) {
				intersect.add(a);
				i++;
				j++;
			} else if (a < b) {
				i++;
			} else {
				j++;
			}
		}
		int[] toRet = intersect.toArray();
		Arrays.sort(toRet);
		return toRet;
	}

	public static int intersectCount(int[] A, int[] B) {
		if (A.length == 0 || B.length == 0)
			return 0;
		// Arrays.sort(shorter);
		// Arrays.sort(longer);
		int iA = 0, iB = 0, intersect = 0;
		while (iA < A.length && iB < B.length) {
			if (A[iA] == B[iB]) {
				iA++;
				iB++;
				intersect++;
			} else if (A[iA] > B[iB]) {
				iB++;
			} else if (A[iA] < B[iB]) {
				iA++;
			}
		}
		return intersect;
	}

	public static void main(String[] args) {
		int[] b = new int[] { 2, 3, 4, 5, 6 };

		Arrays.sort(b);
		System.out.println(intersectCount(new int[] { 1, 2, 3 }, b));
		System.out.println(unionCount(
				new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, new int[] { 2, 3,
						4, 5, 6, 7, 8 }));
		System.out.println(unionCount(new int[] {}, new int[] { 2, 3, 4, 5, 6,
				7, 8, 1, 1, 2, 2, 3, 4, 5, 5 }));
	}

	public static int[] union(int[] l, int[] r) {
		TIntHashSet union = new TIntHashSet();
		union.addAll(l);
		union.addAll(r);
		int[] toRet = union.toArray();
		Arrays.sort(toRet);
		return toRet;
	}

	public static int unionCount(int[] A, int[] B) {
		if (A.length == 0)
			return B.length;
		if (B.length == 0)
			return A.length;
		int iA = 0, iB = 0, union = 0;
		while (iA < A.length && iB < B.length) {
			if (A[iA] == B[iB]) {
				iA++;
				iB++;
			} else if (A[iA] > B[iB]) {
				iB++;
			} else if (A[iA] < B[iB]) {
				iA++;
			}
			union++;
		}

		if (iA < A.length)
			union += A.length - iA;
		if (iB < B.length)
			union += B.length - iB;
		return union;
	}

}
