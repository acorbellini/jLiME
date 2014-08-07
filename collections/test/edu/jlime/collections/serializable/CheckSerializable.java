package edu.jlime.collections.serializable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.junit.Test;

import edu.jlime.collections.intintarray.client.jobs.GetJob;

public class CheckSerializable {

	@Test
	public void test() throws IOException {
		new ObjectOutputStream(new ByteArrayOutputStream())
				.writeObject(new GetJob(0, "b"));
		// Serializable original = SerializationUtils.serialize(new GetJob(1,
		// "a"));
		// Serializable copy = SerializationUtils.clone(original);
		// assertEquals(original, copy);
	}

	public static void main(String[] args) throws IOException {
		new CheckSerializable().test();
	}
}
