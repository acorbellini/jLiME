package edu.jlime.graphly;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;

public interface GraphlyStoreNodeIBroadcast { 

  public void setProperty(final Long arg0, final String arg1, final Object arg2) throws Exception; 

  public Map<Peer,Object>  getProperty(final Long arg0, final String arg1) throws Exception; 

  public Map<Peer,TLongObjectHashMap>  getProperties(final String arg0, final Integer arg1, final TLongArrayList arg2) throws Exception; 

  public void setProperties(final String arg0, final TLongObjectHashMap<java.lang.Object> arg1) throws Exception; 

  public void setTempProperties(final HashMap<java.lang.Long,java.util.Map<java.lang.String, java.lang.Object>> arg0) throws Exception; 

  public void addInEdgePlaceholder(final Long arg0, final Long arg1, final String arg2) throws Exception; 

  public Map<Peer,long[]>  getEdges(final Dir arg0, final Integer arg1, final long[] arg2) throws Exception; 

  public Map<Peer,Long>  getRandomEdge(final Long arg0, final long[] arg1, final Dir arg2) throws Exception; 

  public Map<Peer,Peer>  getJobAddress() throws Exception; 

  public void removeVertex(final Long arg0) throws Exception; 

  public void addRange(final Integer arg0) throws Exception; 

  public Map<Peer,Integer>  getEdgeCount(final Long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public Map<Peer,List>  getRanges() throws Exception; 

  public Map<Peer,Boolean>  addVertex(final Long arg0, final String arg1) throws Exception; 

  public void commitUpdates(final String[] arg0) throws Exception; 

  public void setEdgeProperty(final Long arg0, final Long arg1, final String arg2, final Object arg3, final String[] arg4) throws Exception; 

  public Map<Peer,Object>  getEdgeProperty(final Long arg0, final Long arg1, final String arg2, final String[] arg3) throws Exception; 

  public void addEdge(final Long arg0, final Long arg1, final String arg2, final Object[] arg3) throws Exception; 

  public Map<Peer,String>  getLabel(final Long arg0) throws Exception; 

  public void addEdges(final Long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public Map<Peer,GraphlyCount>  countEdges(final Dir arg0, final Integer arg1, final long[] arg2) throws Exception; 

}