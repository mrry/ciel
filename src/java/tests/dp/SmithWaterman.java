package tests.dp;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class SmithWaterman implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		
		try { 
			
			/**
			 * args: mode          -- one of l, t, tl or i.
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
			while ((c = fis[0].read()) != -1) {
				horizontalChunkBuffer.write(c);
			}
			byte[] horizontalChunk = horizontalChunkBuffer.toByteArray();
			System.err.printf("Horizontal chunk is length: %d\n", horizontalChunk.length);
			
			ByteArrayOutputStream verticalChunkBuffer = new ByteArrayOutputStream();
			while ((c = fis[1].read()) != -1) {
				verticalChunkBuffer.write(c);
			}
			byte[] verticalChunk = verticalChunkBuffer.toByteArray();
			System.err.printf("Vertical chunk is length: %d\n", verticalChunk.length);
			
			// Read input halos (where appropriate).
			int [][] hMatrix = new int[verticalChunk.length + 1][horizontalChunk.length + 1];
							
			if (mode.equals("i")) {
				
				DataInputStream topLeftHaloInputStream = new DataInputStream(fis[1]);
				hMatrix[0][0] = topLeftHaloInputStream.readInt();
				topLeftHaloInputStream.close();
				
				DataInputStream topHaloInputStream = new DataInputStream(fis[2]);
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					hMatrix[0][j] = topHaloInputStream.readInt();
				}
				topHaloInputStream.close();

				DataInputStream leftHaloInputStream = new DataInputStream(fis[2]);
				for (int i = 1; i <= verticalChunk.length; ++i) {
					hMatrix[i][0] = leftHaloInputStream.readInt();
				}
				leftHaloInputStream.close();
				
			} else if (mode.equals("l")) {
				
				DataInputStream topHaloInputStream = new DataInputStream(fis[1]);
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					hMatrix[0][j] = topHaloInputStream.readInt();
				}
				topHaloInputStream.close();
				
			} else if (mode.equals("t")) {
				
				DataInputStream leftHaloInputStream = new DataInputStream(fis[1]);
				for (int i = 1; i <= verticalChunk.length; ++i) {
					hMatrix[i][0] = leftHaloInputStream.readInt();
				}
				leftHaloInputStream.close();
				
			} else if (mode.equals("tl")) {
				
				// Arrays are zero-initialized by default.
				
			} else {
				throw new IllegalArgumentException("Illegal mode specified: " + mode);
			}

			// Array containing scores for each possible action (insert/delete/do nothing).
			int[] currentScores = new int[4];
			
			// Special left-column.
			for (int i = 1; i <= verticalChunk.length; ++i) {
				for (int j = 1; j <= horizontalChunk.length; ++j) {
					if (verticalChunk[i-1] == horizontalChunk[j-1]) {
						// Characters match at this position.
						hMatrix[i][j] = hMatrix[i-1][j-1] + matchScore;
					} else {
						// Characters don't match at this position.
						int bestOption = 0;
						if (bestOption < hMatrix[i-1][j-1] + mismatchScore) {
							bestOption = hMatrix[i-1][j-1] + mismatchScore;
						}
						if (bestOption < hMatrix[i][j-1] + insertionScore) {
							bestOption = hMatrix[i][j-1] + insertionScore;
						}
						if (bestOption < hMatrix[i-1][j] + deletionScore) {
							bestOption = hMatrix[i-1][j] + deletionScore;
						}
						hMatrix[i][j] = bestOption;
					}
				}
			}

			for (int i = 0; i < hMatrix.length; ++i) {
				for (int j = 0; j < hMatrix[i].length; ++j) {
					System.out.printf("%d\t", hMatrix[i][j]);
				}
				System.out.println();
			}
			
			/**
			 * outputs: bottom-right halo, bottom halo, right halo.
			 */
			
			// Write output halos.
			DataOutputStream bottomRightHaloOutputStream = new DataOutputStream(fos[0]);
			bottomRightHaloOutputStream.writeInt(hMatrix[verticalChunk.length][horizontalChunk.length]);
			bottomRightHaloOutputStream.close();
			
			DataOutputStream bottomHaloOutputStream = new DataOutputStream(fos[1]);
			for (int j = 1; j <= horizontalChunk.length; ++j) {
				bottomHaloOutputStream.writeInt(hMatrix[verticalChunk.length][j]);
			}
			bottomHaloOutputStream.close();

			DataOutputStream rightHaloOutputStream = new DataOutputStream(fos[2]);
			for (int i = 1; i <= verticalChunk.length; ++i) {
				rightHaloOutputStream.writeInt(hMatrix[i][horizontalChunk.length]);
			}
			rightHaloOutputStream.close();
			
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
