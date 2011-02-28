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
 * Mandelbrot tile generator class, adapted from CST IA Java Exercise
 * code from 2007.
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
public class Mandelbrot implements Task
{

  private Graphics g;

  /* 
   * Set up some variables
   * 
   * Modify these to change the appearance and zoom level 
   */
  int sWidth = 800;
  int sHeight = 800;
  int tWidth = sWidth;
  int tHeight = sHeight;
  int maxit = 250;
  double scale = 0.005;
  int tOffX = 0;
  int tOffY = 0;

  /*
   * Don't modify these.
   */
  private boolean isFilled = false;
  private BufferedImage p;  
  
  // constructor
  public Mandelbrot()
  {
    
    // Initialize the image buffer    
    p = new BufferedImage(
      sWidth,
      sHeight,
      BufferedImage.TYPE_3BYTE_BGR);  
      // Need TYPE_3BYTE_BGR here to get nice colours

    // Obtain access to a drawing context
    g = p.getGraphics();

  }

  public Mandelbrot(int tWidth, int tHeight, int maxit, double scale, int tOffX, int tOffY)
  {

    // Set tile parameters
    this.tWidth = tWidth;
    this.tHeight = tHeight;
    this.maxit = maxit;
    this.scale = scale;
    this.tOffX = tOffX;
    this.tOffY = tOffY;

    // Initialize the image buffer    
    p = new BufferedImage(
      tWidth,
      tHeight,
      BufferedImage.TYPE_3BYTE_BGR);  
      // Need TYPE_3BYTE_BGR here to get nice colours

    // Obtain access to a drawing context
    g = p.getGraphics();

  }


  @Override
  public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

    if (args.length < 6) {
       System.out.println("usage: java Mandelbrot sizeX sizeY tilesX tilesY myX myY [max iterations] [scale]");
       System.exit(1);
    }

    // create instance
    int sX = Integer.valueOf(args[0]);
    int sY = Integer.valueOf(args[1]);

    int tX = Integer.valueOf(args[2]);
    int tY = Integer.valueOf(args[3]);

    int myX = Integer.valueOf(args[4]);
    int myY = Integer.valueOf(args[5]);

    int maxit = ((args.length > 6) ? Integer.valueOf(args[6]) : 250);
    double scale = ((args.length > 7) ? Double.valueOf(args[7]) : 0.005);


    Mandelbrot m = new Mandelbrot(sX/tX,sY/tY,maxit,scale,myX,myY); 
    m.paint(fos[0]);

  }


  /**
   * main just creates a new instance of the Mandelbrot class
   * @param args Command line arguments (not used)
   */
  public static void main(String[] args) 
  { 

    if (args.length < 6) {
       System.out.println("usage: java Mandelbrot sizeX sizeY tilesX tilesY myX myY [max iterations] [scale]");
       System.exit(1);
    }

    // create instance
    int sX = Integer.valueOf(args[0]);
    int sY = Integer.valueOf(args[1]);

    int tX = Integer.valueOf(args[2]);
    int tY = Integer.valueOf(args[3]);

    int myX = Integer.valueOf(args[4]);
    int myY = Integer.valueOf(args[5]);

    int maxit = ((args.length > 6) ? Integer.valueOf(args[6]) : 250);
    double scale = ((args.length > 7) ? Double.valueOf(args[7]) : 0.005);


    Mandelbrot m = new Mandelbrot(sX/tX,sY/tY,maxit,scale,myX,myY); 
    m.paint();
  }

  /**
   * paint function draws/updates the image buffer as necessary
   */
  public void paint()
  {
    if(!isFilled) fillImage();

    File outFile = new File("tile" + tOffX + "-" + tOffY + ".png");
    try {
        ImageIO.write(p, "png", outFile);
    } catch (IOException e) {
        System.out.println("Failed to write tile image!");
    }
    
  }

  /**
   * paint function draws/updates the image buffer as necessary
   */
  public void paint(OutputStream os)
  {
    if(!isFilled) fillImage();

    try {
        ImageIO.write(p, "png", os);
    } catch (IOException e) {
        System.out.println("Failed to write tile image!");
    }

    
  }
  
  /**
   * function to draw the image to the buffer
   */
  void fillImage()
  {
    Graphics g = p.getGraphics();
    int localI;
    int localJ;    
    int globalI = tWidth*tOffX;
    int globalJ = tHeight*tOffY;    

    for(localI=0; localI<tWidth; localI++) {
      for(localJ=0; localJ<tHeight; localJ++) {
        // For each pixel on the screen do: 
        g.setColor(getPixel((globalI+localI-(sWidth/2))*scale,(globalJ+localJ-(sHeight/2))*scale, maxit));
        g.drawRect(localI, localJ, 1, 1);
      }
    }
    
    isFilled = true;
  }

  /**
   * getPixel calculates the colour of each pixel in the image
   * @param x0 Initial value x
   * @param y0 Initial value y
   * @param maxit Maximum number of iterations
   * @return Color Pixel colour
   */
  private Color getPixel(double x0, double y0, int maxit) 
  {
    double x = 0.0;
    double y = 0.0;
  
    double x2 = x*x;
    double y2 = y*y;
      
    int it = 0;
   
    while ( x2 + y2 < 4  &&  it < maxit ) 
    {
      y = 2*x*y + y0;
      x = x2 - y2 + x0;
  
      x2 = x*x;
      y2 = y*y;
  
      it++;
    }
   
    if ( it == maxit ) {
      // make surrounding area black
      return Color.BLACK;       
    } else {
      // print blue-ish image - very simply-minded algorithm 
      // exploiting the Color(int) constructor
      //return new Color(it);
      
      // Algorithm stolen from lecture notes - this gives nice 
      // colourful images
      return Color.getHSBColor( (float)(it%64.0)/64.0f , 
                      (float)(0.6+0.4*Math.cos( (double)it/40.0 ) ), 
                      1.0f);
      
      // use Color.WHITE to get the original mathematical image with b/w areas
      //return Color.WHITE;
    }
  }
  
}

