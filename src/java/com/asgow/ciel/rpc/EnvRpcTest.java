package com.asgow.ciel.rpc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.StdinoutTaskInformation;

public class EnvRpcTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		WorkerRpc rpc = new JsonPipeRpc(System.getenv("CIEL_PIPE_TO_WORKER"), System.getenv("CIEL_PIPE_FROM_WORKER"));
		Reference[] result = rpc.spawnTask(new StdinoutTaskInformation(null, new String[] { "wc", "-w", "/usr/share/dict/words" }));
		
		System.out.println(result[0]);
		
		Ciel.blockOn(result);
		
		String childFile = rpc.getFilenameForReference(result[0]);
		
		try {
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(childFile)));
			
			String line;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
			}
			
			br.close();
			
			WritableReference outFile = rpc.getOutputFilename(0);
			
			OutputStreamWriter osw = (new OutputStreamWriter(outFile.open()));
			osw.write("Hello world!");
			osw.close();
			
		} catch (IOException ioe) {
			
		}
		
		rpc.exit(false);
		
	}
		

}
