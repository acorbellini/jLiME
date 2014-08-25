jLiME
=====

A Java Lightweight Execution Framework.

This a support project to program my PhD experiments. 


#Cluster-based

This implementation is based on the notion of a Cluster. A Cluster object is usually used to track membership and to query for existing members. The Transport implementation is required to provide some sort of peer discovery.

#RPCDispatcher

The RPCDispatcher class is one of the central parts of the project, it manages remote class loading and method calling.

##RPC Creator

The RPC Creator class creates supporting classes for making rpc calls (pretty much like RMI and you can see what's being generated). Receives a list of interfaces that are used to generate the RPC clients. Two special annotations can be used on the interfaces' methods: 
* @Sync : indicates that is a synchronous call(even if it returns void).
* @Cache : saves the result of the rpc call in a variable so that subsequent calls use that variable.

#jLiME Transport

A custom Transport implementation that can be configured to use UDP, TCP and/or UDP multicast.

#jLiMEFactory

Factory that configures and creates an RPCDispatcher using the jLiME Transport implementation.

##JobDispatcher

A class that uses RPCDispatcher to call other JobDispatcher to execute Jobs.



