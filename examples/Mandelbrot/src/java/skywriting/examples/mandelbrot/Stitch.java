package skywriting.examples.mandelbrot;

/*
 * Copyright (c) 2011 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * ----
 *
 * Naive image stitching class for use with the Mandelbrot example code.
 * This is quite hacked-up, so don't judge me on it ;-)
 * 
 */

/*
 * Imports
 */

import uk.co.mrry.mercator.task.Task;

import java.awt.*;
import java.awt.image.BufferedImage;
import javax.imageio.*;
import java.io.*;

/**
 * @author malte
 *
 */
public class Stitch implements Task
{

  private Graphics g;

  /* 
   * Set up some variables
   */
  int nX = 5;
  int nY = 5;
  int tWidth = 200;
  int tHeight = 200;

  /*
   * Don't modify these.
   */
  private BufferedImage out;
  private BufferedImage[] tiles;
  private InputStream[] tileStreams;
  

  public Stitch() {
    assert(false);
  }

  // constructor
  public Stitch(int nTilesX, int nTilesY, int tWidth, int tHeight, InputStream[] tiles)
  {

    // Set tile parameters
    nX = nTilesX;
    nY = nTilesY;
    this.tWidth = tWidth;
    this.tHeight = tHeight;
    tileStreams = tiles;
    
    // Initialize the image buffer    
    out = new BufferedImage(
      tWidth*nTilesX,
      tHeight*nTilesY,
      BufferedImage.TYPE_3BYTE_BGR);  
      // Need TYPE_3BYTE_BGR here to get nice colours

    // Obtain access to a drawing context
    g = out.getGraphics();

  }

  @Override
  public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

    if (args.length < 4) {
       System.out.println("usage: java Stitch numTilesX numTilesY totalWidth totalHeight");
       System.exit(1);
    }

    // create instance
    int nX = Integer.valueOf(args[0]);
    int nY = Integer.valueOf(args[1]);

    int sX = Integer.valueOf(args[2]);
    int sY = Integer.valueOf(args[3]);

    int tX = sX/nX;
    int tY = sY/nY;

    InputStream[] tiles;

    Stitch s = new Stitch(nX,nY,tX,tY,fis); 
    s.stitch(fos[0]);

  }


  /**
   * main just creates a new instance of the Mandelbrot class
   * @param args Command line arguments (not used)
   */
  public static void main(String[] args) 
  { 

    if (args.length < 4) {
       System.out.println("usage: java Stitch numTilesX numTilesY totalWidth totalHeight");
       System.exit(1);
    }

    // create instance
    int nX = Integer.valueOf(args[0]);
    int nY = Integer.valueOf(args[1]);

    int sX = Integer.valueOf(args[2]);
    int sY = Integer.valueOf(args[3]);

    int tX = sX/nX;
    int tY = sY/nY;

    InputStream[] tiles = new InputStream[nX*nY];

    try {
       // open the corresponding files
       for (int i = 0; i < nX; i++) {
           for (int j = 0; j < nY; j++) {
               tiles[i*nX+j] = new FileInputStream("tile" + i + "-" + j + ".png");
           }
       }

       FileOutputStream out = new FileOutputStream("final.png");

       Stitch s = new Stitch(nX,nY,tX,tY,tiles); 
       s.stitch(out);
    } catch (Exception e) {
       System.out.println("Error!");
       System.exit(1);
    }
  }

  /**
   * paint function draws/updates the image buffer as necessary
   */
  public void stitch(OutputStream os)
  {

    // do the stitching
    for (int i = 0; i < nX; i++) {
        for (int j = 0; j < nY; j++) {
            try {
                BufferedImage t = ImageIO.read(tileStreams[i*nX+j]);
                if (!g.drawImage(t, i*tWidth, j*tHeight, null)) {
                    System.exit(1);	
                }
            } catch (IOException e) {
                System.out.println("Failed reading tile (" + i + ", " + j +")!");
            }
        }
    }

    try {
        ImageIO.write(out, "png", os);
    } catch (IOException e) {
        System.out.println("Failed to write tile image!");
    }

  }
  
}
