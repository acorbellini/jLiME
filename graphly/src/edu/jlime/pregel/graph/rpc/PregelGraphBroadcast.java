package edu.jlime.pregel.graph.rpc;

import java.util.List;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.pregel.worker.VertexList;
import edu.jlime.pregel.worker.VertexList;
import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.set.hash.TLongHashSet;
import gnu.trove.set.hash.TLongHashSet;

public interface PregelGraphBroadcast { 

   public Map<Peer,Object>  get(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final float arg2) throws Exception; 

   public Map<Peer,Float>  getFloat(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,Double>  getDouble(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,String>  getName() throws Exception; 

   public Map<Peer,String>  print() throws Exception; 

   public void flush(final VertexList arg0) throws Exception; 

   public void setDouble(final long arg0, final String arg1, final double arg2) throws Exception; 

   public void setFloat(final long arg0, final String arg1, final float arg2) throws Exception; 

   public Map<Peer,Object>  getDefaultValue(final String arg0) throws Exception; 

   public void setVal(final long arg0, final String arg1, final Object arg2) throws Exception; 

   public void disable(final long arg0) throws Exception; 

   public Map<Peer,Float>  getDefaultFloat(final String arg0) throws Exception; 

   public Map<Peer,Integer>  getNeighbourhoodSize(final long arg0) throws Exception; 

   public void putLink(final long arg0, final long arg1) throws Exception; 

   public void putIncoming(final long arg0, final long arg1) throws Exception; 

   public void removeOutgoing(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,TLongHashSet>  getNeighbours(final long arg0) throws Exception; 

   public Map<Peer,Boolean>  createVertex(final long arg0) throws Exception; 

   public Map<Peer,Integer>  getAdyacencySize(final long arg0) throws Exception; 

   public Map<Peer,Integer>  getOutgoingSize(final long arg0) throws Exception; 

   public void setDefaultValue(final String arg0, final Object arg1) throws Exception; 

   public Map<Peer,TLongHashSet>  getOutgoing(final long arg0) throws Exception; 

   public void preload(final VertexList arg0) throws Exception; 

   public void disableOutgoing(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,TLongHashSet>  getAdjacents(final long arg0, final Dir arg1) throws Exception; 

   public void createVertices(final Set<java.lang.Long> arg0) throws Exception; 

   public void enableAll() throws Exception; 

   public void putOutgoing(final List<long[]> arg0) throws Exception; 

   public void putOutgoing(final long arg0, final long arg1) throws Exception; 

   public void disableLink(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,TLongHashSet>  getIncoming(final long arg0) throws Exception; 

   public void disableIncoming(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,Boolean>  isLocal(final long arg0) throws Exception; 

   public Map<Peer,Iterable>  vertices() throws Exception; 

   public Map<Peer,Integer>  vertexSize() throws Exception; 

}