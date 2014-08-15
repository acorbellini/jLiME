package edu.jlime.collections.intintarray.loader;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class MultiThreadDKVSLoader extends Loader {

	public MultiThreadDKVSLoader(String propFilePath) {
		super(propFilePath);
	}

	ExecutorService exec = Executors.newFixedThreadPool(20);

	Semaphore sem = new Semaphore(40);

	@Override
	protected void agregar(final int k, final int[] list) throws IOException {
		try {
			sem.acquire();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		exec.execute(new Runnable() {
			@Override
			public void run() {
				try {
					MultiThreadDKVSLoader.super.agregar(k, list);
					sem.release();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
	}

	@Override
	protected void finalize() throws Throwable {
		exec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
		exec.shutdown();
	}

	public static void main(String[] args) throws Exception {
		new MultiThreadDKVSLoader(args[0]).load();
		System.exit(0);
	}
}
