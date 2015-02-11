package edu.jlime.graphly;

import java.util.List;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.core.cluster.Peer;
import gnu.trove.map.hash.TLongIntHashMap;

public interface GraphlyStoreNodeIBroadcast { 

  public void setProperty(final Long arg0, final String arg1, final Object arg2) throws Exception; 

  public Map<Peer,Object>  getProperty(final Long arg0, final String arg1) throws Exception; 

  public void addInEdgePlaceholder(final Long arg0, final Long arg1, final String arg2) throws Exception; 

  public void addEdge(final Long arg0, final Long arg1, final String arg2, final Object[] arg3) throws Exception; 

  public void removeVertex(final Long arg0) throws Exception; 

  public Map<Peer,Boolean>  addVertex(final Long arg0, final String arg1) throws Exception; 

  public void addEdges(final Long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public Map<Peer,TLongIntHashMap>  countEdges(final Dir arg0, final long[] arg1) throws Exception; 

  public Map<Peer,Long>  getRandomEdge(final Long arg0, final Dir arg1) throws Exception; 

  public Map<Peer,Object>  getEdgeProperty(final Long arg0, final Long arg1, final String arg2, final String[] arg3) throws Exception; 

  public void setEdgeProperty(final Long arg0, final Long arg1, final String arg2, final Object arg3, final String[] arg4) throws Exception; 

  public Map<Peer,long[]>  getEdges(final Dir arg0, final long[] arg1) throws Exception; 

  public Map<Peer,List>  getEdges(final Long arg0, final Dir arg1, final String[] arg2) throws Exception; 

  public void addRange(final Integer arg0) throws Exception; 

  public Map<Peer,List>  getRanges() throws Exception; 

  public Map<Peer,String>  getLabel(final Long arg0) throws Exception; 

  public Map<Peer,Peer>  getJobAddress() throws Exception; 

}