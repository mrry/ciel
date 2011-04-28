import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BinomialOptions {

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

  static public double calculate(DataInputStream sin, DataOutputStream sout, double s, double k, double t, double v, double rf, double cp, int n, int chunk, boolean start) throws IOException {
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
        return r;
      } else {
        int rowto = Math.max(0, rowstart - chunk);
        process_rows(sout, sin, rowstart, rowto, q, drift);
      }
    }
    return(0.0);
  }
}

