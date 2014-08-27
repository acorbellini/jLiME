package edu.jlime.core.marshallerTest;

import org.junit.Test;

public class MarshallerTest {

	@Test
	public void test() throws Exception {
		// Marshaller m = new Marshaller();
		// byte[] marshalled = m.objectToBuffer(
		// Compression.compress(ExecutionClient
		// .serializeClass(new MyRealJob(4)))).getBuf();
		// // byte[] marshalled = m.objectToBuffer(MyRealJob.class).getBuf();
		//
		// // Class job = (Class) m
		// // .objectFromBuffer(marshalled, 0, marshalled.length);
		//
		// byte[] unmarshalled = (byte[]) m.objectFromBuffer(marshalled, 0,
		// marshalled.length);
		// byte[] uncompressed = Compression.uncompress(unmarshalled);
		// // NetworkClassLoader loader = new NetworkClassLoader();
		// // loader.addClassAsBytes(job, MyRealJob.class.getName());
		// // Class myjobclass = loader.loadClass(MyRealJob.class.getName());
		// // Constructor constructor = job.getConstructor(Integer.class);
		// //
		// // Callable myjob = (Callable) constructor.newInstance(new
		// Integer(4));
		// // myjob.call();

		// Marshaller m = new Marshaller(new NetworkClassLoader());
		// byte[] marshalled = m.objectToBuffer(
		// ExecutionClient.serializeClass(new MyRealJob(4))).getBuf();
		// TByteArrayList unmarshalled = (TByteArrayList) m.objectFromBuffer(
		// marshalled, 0, marshalled.length);
		// NetworkClassLoader loader = new NetworkClassLoader();
		// loader.addClassAsBytes(unmarshalled.toArray(),
		// MyRealJob.class.getName());
		// Class myjobclass = loader.loadClass(MyRealJob.class.getName());
		// Constructor constructor = myjobclass.getConstructor(Integer.class);
		//
		// Callable myjob = (Callable) constructor.newInstance(new Integer(4));
		// myjob.call();
	}
}
