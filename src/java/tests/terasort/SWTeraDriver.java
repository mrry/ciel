import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;


public class SWTeraDriver {

	public static void main(String[] args) {

		try {

			if(args.length == 0) {
				System.out.println("Usage: java SWTeraDriver (merger | bucketer | sampler)");
				System.exit(1);				
			}
			if(args[0].equals("sampler")) {

				if(args.length != 4) {
					System.out.println("Usage: java SWTeraDriver sampler nPartitions datafileName partitionFileName");
					System.exit(1);
				}

				String[] samplerArgs = new String[2];
				samplerArgs[1] = args[1];
				File f = new File(args[2]);
				samplerArgs[0] = ((Long)f.length()).toString();
				InputStream[] inputs = new InputStream[1];
				inputs[0] = new FileInputStream(args[2]);
				OutputStream[] outputs = new OutputStream[1];
				outputs[0] = new FileOutputStream(args[3]);

				new SWTeraSampler().invoke(inputs, outputs, samplerArgs);

			}
			else if(args[0].equals("bucketer")) {
				if(args.length < 5) {
					System.out.println("Usage: java SWTeraDriver bucketer nMergers partitionFileName inputDataFileName reducerOutputPath");
					System.exit(1);
				}
				InputStream[] inputs = new InputStream[2];
				inputs[0] = new FileInputStream(args[2]);
				inputs[1] = new FileInputStream(args[3]);
				String[] bucketerArgs = new String[1];
				bucketerArgs[0] = args[1];
				int nReducers = Integer.parseInt(args[1]);
				OutputStream[] outputs = new OutputStream[nReducers];
				for(int i = 0; i < nReducers; i++) {
					outputs[i] = new FileOutputStream(args[4] + "/" + ((Integer)i).toString()); 
				}

				new SWTeraBucketer().invoke(inputs, outputs, bucketerArgs);
			}
			else if(args[0].equals("merger")) {
				if(args.length < 3) {
					System.out.println("Usage: java SWTeraDriver merger nBucketers outputFileName inputFileName1 [...]");
					System.exit(1);
				}
				
				String[] mergerArgs = new String[1];
				mergerArgs[0] = args[1];
				OutputStream[] outputs = new OutputStream[1];
				outputs[0] = new FileOutputStream(args[2]);
				int nBucketers = Integer.parseInt(args[1]);
				if(args.length < (nBucketers + 3)) {
					System.out.println("Usage: java SWTeraDriver merger nBucketers outputFileName inputFileName1 [...]");
					System.out.println("Number of bucketer files must match nBucketers");
					System.exit(1);					
				}
				InputStream[] inputs = new InputStream[nBucketers];
				for(int i = 0; i < nBucketers; i++) {
					inputs[i] = new FileInputStream(args[3 + i]);
				}
				
				new SWTeraMerger().invoke(inputs, outputs, mergerArgs);
				
			}
			
			else {
				System.out.println("Usage: java SWTeraDriver (merger | bucketer | sampler)");
				System.exit(1);
			}
		}
		catch(Exception e) {

			System.err.println("Exception: " + e);
			System.exit(1);

		}

	}

}