jLiME
=====

A Java Lightweight Execution Framework.  
  
Refer to each sub-project for details:  
  
- collections: some distributed collections I used in the past (based on jobdispatcher)  
- core: core classes for marshalling, rpc, streaming, transport abstractions  
- graphly: a graph store and processing tool  
- gridgain: gridgain conector (in progress...)
- httpserver: old web console
- jgroups: jgroups support (outdated)
- jlime-pregel: pregel model support for graphly
- jlime-rpc: actually, this should be jlime-network-stack, or something like that. It's an stack of network tools for connecting java applications. Provides UDP, UDP (reliable), TCP and the NIO versions of them. Additionally, rabbitmq and zeromq connectors are provided.
- jobdispatcher: A generic Job execution tool. It executes Jobs remotely. 
- linkprediction: Some link-prediction algorithms for my research.
- metrics: A simple, yet powerful metric library based on Metrics.
- util: Utilility classes. The most interesting ones are a Simple CLI parser and a Table for processing CSV files. Compression  - classes are very useful too.
