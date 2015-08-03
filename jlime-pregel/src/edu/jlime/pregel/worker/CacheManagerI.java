package edu.jlime.pregel.worker;

public interface CacheManagerI {

	void flush() throws Exception;

	void send(String type, long v, long to, Object curr) throws Exception;

	void sendAll(String msgType, long v, Object val) throws Exception;

	void sendFloat(String type, long v, long to, float curr) throws Exception;

	void sendAllFloat(String msgType, long v, float val) throws Exception;

	void sendAllDouble(String msgType, long v, double val) throws Exception;

	void sendDouble(String type, long v, long to, double val) throws Exception;

}
