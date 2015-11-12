package edu.jlime.pregel.worker;

import java.util.UUID;

public interface CacheManagerI {

	void flush() throws Exception;

	void send(String type, long v, long to, Object curr) throws Exception;

	void sendAll(String msgType, long v, Object val) throws Exception;

	void sendFloat(UUID wid, String type, long v, long to, float curr) throws Exception;

	void sendAllFloat(String msgType, long v, float val) throws Exception;

	void sendAllDouble(String msgType, long v, double val) throws Exception;

	void sendDouble(String type, long v, long to, double val) throws Exception;

	void stop();

	void sendAllSubGraph(String msgType, String subgraph, long v, Object val) throws Exception;

	void sendAllFloatSubGraph(String msgType, String subgraph, long v, float val) throws Exception;

	void mergeWith(CacheManagerI cache) throws Exception;

}
