package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobContainer;

public interface JobExecutorBroadcast { 

  public void execute(final JobContainer arg0) throws Exception; 

  public void result(final Object arg0, final UUID arg1, final ClientNode arg2) throws Exception; 

}