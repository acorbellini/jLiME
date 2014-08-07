package edu.jlime.core.rpc;

import org.apache.log4j.Logger;

public class AsyncLogger {

	private static Logger logger = Logger.getLogger(AsyncLogger.class);

	public static void logAsyncException(Exception obj) {
		logger.error("Async Error", obj);
	}

}
