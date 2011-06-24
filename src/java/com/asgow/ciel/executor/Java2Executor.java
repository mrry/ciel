package com.asgow.ciel.executor;

import com.asgow.ciel.rpc.JsonPipeRpc;
import com.asgow.ciel.rpc.ShutdownException;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class Java2Executor {

	public static void main(String[] args) {

		if(args[0].equals("--version")) {
			System.out.println("Ciel Java2Executor bindings v0.2\n");
			return;
		}

		if(args.length < 4) {
			System.err.println("Usage: java com.asgow.ciel.executor.Java2Executor --write-fifo WRITE_FIFO_NAME --read-fifo READ_FIFO_NAME");
			System.exit(1);
		}
		
		assert args[0].equals("--write-fifo");
		assert args[2].equals("--read-fifo");
		
		String writeFifoName = args[1];
		String readFifoName = args[3];
		
		Ciel.RPC = new JsonPipeRpc(writeFifoName, readFifoName);
		Ciel.softCache = new SoftCache();

		while(true) {

			FirstClassJavaTask task = null;
			//Ciel.softCache.sweepCache();
			try {
				task = Ciel.RPC.getTask();
			} catch (ShutdownException e) {
				//System.out.println("Java2Executor: ordered to shut down (reason: '" + e.reason + "')");
				System.exit(0);
			}
			
			try {
				task.setup();
				task.invoke();
				Ciel.RPC.exit(false);
			} catch (Exception e) {
				e.printStackTrace();
				Ciel.RPC.error(e.toString());
				System.exit(0);
			}
			
		}

	}

}