package skywriting.examples.smithwaterman;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;

import uk.co.mrry.mercator.task.Task;

public class SmithWaterman implements Task {

    private List<String> trace_io;

    public SmithWaterman() {

	if(System.getProperty("skywriting.trace_io") != null) {
	    System.err.printf("*** DEBUG: Outputting IO trace data\n");
	    trace_io = new LinkedList<String>();
	}
	else {
	    System.err.printf("Smith-waterman: normal startup\n");
	    trace_io = null;
	}

    }

    public void changeState(boolean compute_bound, int ios) {
	if(trace_io != null) {
	    if(compute_bound) {
		trace_io.add(String.format("C%f,", ((double)System.currentTimeMillis()) / 1000));
	    }
	    else {
		trace_io.add(String.format("I%d|%f,", ios, ((double)System.currentTimeMillis()) / 1000));
	    }
	}
    }

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		
		try { 
			
			/**
			 * args: mode           -- one of l, t, tl or i.
			 *       insertionScore -- integer score for inserting a character.
			 *       deletionScore  -- integer score for deleting a character.
			 *       mismatchScore  -- integer score for a mismatch.
			 *       matchScore     -- integer score for a match.
			 */
			
			// mode is one of l (left-hand edge), t (top edge), tl (top left-hand corner) or i
			// We use this to determine the starting conditions.
			String mode = args[0];
			
			int insertionScore = Integer.parseInt(args[1]);
			int deletionScore = Integer.parseInt(args[2]);
			int mismatchScore = Integer.parseInt(args[3]);
			int matchScore = Integer.parseInt(args[4]);
			
			/**
			 * inputs: "i" -> horizontal input chunk, vertical input chunk, top-left halo, top halo, left halo.
			 *         "l" -> horizontal input chunk, vertical input chunk, top halo.
			 *         "t" -> horizontal input chunk, vertical input chunk, left halo.
			 *         "tl"-> horizontal input chunk, vertical input chunk.
			 */
			
			// Read input chunks.
			int c;
			
			ByteArrayOutputStream horizontalChunkBuffer = new ByteArrayOutputStream();
			changeState(false, 0);
			while ((c = fis[0].read()) != -1) {
				horizontalChunkBuffer.write(c);
			}
			changeState(true, 0);
			byte[] horizontalChunk = horizontalChunkBuffer.toByteArray();
			System.err.printf("Horizontal chunk is length: %d\n", horizontalChunk.length);
			
			ByteArrayOutputStream verticalChunkBuffer = new ByteArrayOutputStream();
			changeState(false, 1);
			while ((c = fis[1].read()) != -1) {
				verticalChunkBuffer.write(c);
			}
			changeState(true, 0);
			byte[] verticalChunk = verticalChunkBuffer.toByteArray();
			System.err.printf("Vertical chunk is length: %d\n", verticalChunk.length);
			
			// Read input halos (where appropriate).
			//int[] leftHalo = new int[verticalChunk.length + 1];
			int left = 0;
			int[] previousRow = new int[horizontalChunk.length + 1];
			
			int leftHaloIndex = 0;
			DataInputStream leftHaloInputStream;
			
			if (mode.equals("i")) {
				DataInputStream topLeftHaloInputStream = new DataInputStream(fis[2]);
				changeState(false, 2);
				previousRow[0] = topLeftHaloInputStream.readInt();
				changeState(true, 0);
				left = previousRow[0];
				topLeftHaloInputStream.close();
				
				DataInputStream topHaloInputStream = new DataInputStream(fis[3]);
				changeState(false, 3);
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					previousRow[j] = topHaloInputStream.readInt();
				}
				changeState(true, 0);
				topHaloInputStream.close();

				leftHaloInputStream = new DataInputStream(fis[4]);
				leftHaloIndex = 4;
				
			} else if (mode.equals("l")) {
				
				DataInputStream topHaloInputStream = new DataInputStream(fis[2]);
				changeState(false, 2);
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					previousRow[j] = topHaloInputStream.readInt();
				}
				changeState(true, 0);
				topHaloInputStream.close();
				
				leftHaloInputStream = new DataInputStream(new ZeroInputStream());
				leftHaloIndex = 0;
				
			} else if (mode.equals("t")) {

				leftHaloInputStream = new DataInputStream(fis[2]);
				leftHaloIndex = 2;
				
			} else if (mode.equals("tl")) {

				// Arrays are zero-initialized by default.
				leftHaloInputStream = new DataInputStream(new ZeroInputStream());
				leftHaloIndex = 0;
			
			} else {
				throw new IllegalArgumentException("Illegal mode specified: " + mode);
			}

			int[] currentRow = new int[previousRow.length];

			DataOutputStream rightHaloOutputStream = new DataOutputStream(fos[2]);

			if(leftHaloIndex != 0)
			    changeState(false, leftHaloIndex);
			for (int i = 1; i <= verticalChunk.length; ++i) {
				int aboveLeft = left;
				left = leftHaloInputStream.readInt();
				
				previousRow[0] = aboveLeft;
				currentRow[0] = left;
				
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					if (verticalChunk[i-1] == horizontalChunk[j-1]) {
						// Characters match at this position.
						currentRow[j] = previousRow[j-1] + matchScore;
					} else {
						// Characters don't match at this position.
						int bestOption = 0;
						if (bestOption < previousRow[j-1] + mismatchScore) {
							bestOption = previousRow[j-1] + mismatchScore;
						}
						if (bestOption < currentRow[j-1] + insertionScore) {
							bestOption = currentRow[j-1] + insertionScore;
						}
						if (bestOption < previousRow[j] + deletionScore) {
							bestOption = previousRow[j] + deletionScore;
						}
						currentRow[j] = bestOption;
					}
				}
				
				rightHaloOutputStream.writeInt(currentRow[horizontalChunk.length]);
				
				int[] temp;
				temp = currentRow;
				currentRow = previousRow;
				previousRow = temp;
			}
			if(leftHaloIndex != 0)
			    changeState(true, 0);
			
			leftHaloInputStream.close();
			
			/**
			 * outputs: bottom-right halo, bottom halo, right halo.
			 */
			
			// Write output halos.
			{
				DataOutputStream bottomRightHaloOutputStream = new DataOutputStream(fos[0]);
				bottomRightHaloOutputStream.writeInt(previousRow[horizontalChunk.length]);
				bottomRightHaloOutputStream.flush();
				bottomRightHaloOutputStream.close();
			}
			
			{
				DataOutputStream bottomHaloOutputStream = new DataOutputStream(fos[1]);
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					bottomHaloOutputStream.writeInt(previousRow[j]);
				}
				bottomHaloOutputStream.flush();
				bottomHaloOutputStream.close();
			}
			
			{
				rightHaloOutputStream.flush();
				rightHaloOutputStream.close();
			}

			if(trace_io != null) {
			    for(String s : trace_io) {
				System.out.print(s);
				System.out.flush();
			    }
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) {
		try {
			FileInputStream horizontalStringInput = new FileInputStream(args[0]);
			FileInputStream verticalStringInput = new FileInputStream(args[1]);

			ByteArrayOutputStream bottomRightHaloOutput = new ByteArrayOutputStream();
			ByteArrayOutputStream bottomHaloOutput = new ByteArrayOutputStream();
			ByteArrayOutputStream rightHaloOutput = new ByteArrayOutputStream();
			
			new SmithWaterman().invoke(
					new InputStream[] { horizontalStringInput, verticalStringInput },
					new OutputStream[] { bottomRightHaloOutput, bottomHaloOutput, rightHaloOutput },
					new String[] { "tl", "-1", "-1", "-1", "2" });
			
			int result = new DataInputStream(new ByteArrayInputStream(bottomRightHaloOutput.toByteArray())).readInt();
			
			System.out.println("Completed Smith-Waterman. Score = " + result);
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
}
