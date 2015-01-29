package edu.jlime.core.compression;

import org.junit.Test;

import edu.jlime.util.compression.SnappyComp;

public class CompressionTest {

	@Test
	public void test() {
		int size = 1000;
		int randomnes = 300;
		int[] testarray = new int[size];
		for (int i = 0; i < testarray.length; i++) {
			testarray[i] = (int) (Math.random() * randomnes);
		}

		byte[] compressed = SnappyComp.compress(testarray);
		System.out.println(compressed.length);
		System.out.println(testarray.length * 4);

		int[] uncompressed = SnappyComp.uncompressIntArray(compressed);

		System.out.println(uncompressed.length * 4);
		System.out.println(testarray.length * 4);

	}
}
