package skywriting.examples.tests.java2;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class BinomialOptions implements FirstClassJavaTask {
	
  private Reference input;
  private double init_s;
  private double init_k;
  private double init_t;
  private double init_v;
  private double init_rf;
  private double init_cp;
  private int init_n;
  private int init_chunk;
  private boolean should_start;
  private boolean use_direct_fifos;
	
  public BinomialOptions(Reference input, double s, double k, double t, double v, double rf, double cp, int n, int chunk, boolean start, boolean direct_fifos) {
	  
	this.input = input;
	this.init_s = s;
	this.init_k = k;
	this.init_t = t;
	this.init_v = v;
	this.init_rf = rf;
	this.init_cp = cp;
	this.init_n = n;
	this.init_chunk = chunk;
	this.should_start = start;
	this.use_direct_fifos = direct_fifos;
	  
  }
  
  @Override
  public void setup() {
  	
  }

  @Override
  public void invoke() throws Exception {
	  
	WritableReference out_ref = Ciel.RPC.getOutputFilename(0, true, this.use_direct_fifos, false);
	OutputStream out = out_ref.open();
	DataOutputStream data_out = new DataOutputStream(out);
	InputStream in = null;
	DataInputStream data_in = null;
	
	if(this.input != null) {
		in = Ciel.RPC.getStreamForReference(this.input, 1, true, false, false);
		data_in = new DataInputStream(in);
	}
	
	calculate(data_in, data_out, this.init_s, this.init_k, this.init_t, this.init_v, this.init_rf, this.init_cp, this.init_n, this.init_chunk, this.should_start);
	
	if(data_in != null) {
		data_in.close();
	}
	data_out.close();
	  
  }

  @Override
  public Reference[] getDependencies() {
	if(this.input != null) {
		Reference[] refs = new Reference[1];
		refs[0] = this.input;
		return refs;
	}
	else {
		return new Reference[0];
	}
  }

  private static double c_stkval(double n, double s, double u, double d, double j) {
    return ( s * (Math.pow(u, (n-j))) * (Math.pow(d,j)));
  }

  private static void gen_initial_optvals(DataOutputStream sout, int n, double s, double u, double d, double k, double cp) throws IOException {
    for (int j=n; j>=0; j--) {
      double stkval = c_stkval (n,s,u,d,j);
      double v = Math.max(0, (cp * (stkval - k)));
      sout.writeDouble(v);
    }
  }

  private static double eqn(double q, double drift, double a, double b) {
    return (((q * a) + (1.0 - q) * b) / drift);
  }

  private static void apply_column(DataOutputStream sout, double v, double[] acc, int pos, int chunk, double q, double drift) throws IOException {
    double v1 = acc[0];
    acc[0] = v;
    int maxcol = Math.min(chunk, pos);
    for (int idx=1; idx<maxcol+1; idx++) {
      double nv1 = eqn(q, drift, acc[idx-1], v1);
      v1 = acc[idx];
      acc[idx] = nv1;
    }
    if (maxcol == chunk)
      sout.writeDouble(acc[maxcol]);
  }

  private static void process_rows(DataOutputStream sout, DataInputStream sin, int rowstart, int rowto, double q, double drift) throws IOException {
    sout.writeInt(rowto);
    int chunk = rowstart - rowto;
    double[] acc = new double[chunk+1];
    for (int pos=0; pos<rowstart+1; pos++) {
      double v = sin.readDouble();
      apply_column(sout, v, acc, pos, chunk, q, drift);
    }
  }

  static public void calculate(DataInputStream sin, DataOutputStream sout, double s, double k, double t, double v, double rf, double cp, int n, int chunk, boolean start) throws IOException {
    double h = t / n;
    double xd = (rf - 0.5 * (v * v)) * h;
    double xv = v * Math.sqrt(h);
    double u = Math.exp ( xd + xv );
    double d = Math.exp ( xd - xv );
    double drift = Math.exp (rf * h);
    double q = (drift - d) / (u - d);

    if (start) {
      sout.writeInt(n);
      gen_initial_optvals(sout, n, s, u, d, k, cp);
    } else {
      int rowstart = sin.readInt();
      if (rowstart == 0) {
        double r = sin.readDouble();
        sout.writeDouble(r);
      } else {
        int rowto = Math.max(0, rowstart - chunk);
        process_rows(sout, sin, rowstart, rowto, q, drift);
      }
    }
  }
  
}

