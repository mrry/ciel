import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class BinomialRun {
  public static void main(String[] args) throws IOException {
     double s = Double.parseDouble(args[0]);
     double k = Double.parseDouble(args[1]);
     double t = Double.parseDouble(args[2]);
     double v = Double.parseDouble(args[3]);
     double rf = Double.parseDouble(args[4]);
     double cp = Double.parseDouble(args[5]);
     int n = Integer.parseInt(args[6]);
     int chunk = Integer.parseInt(args[7]);
     boolean start = false;
     if (Integer.parseInt(args[8]) > 0)
       start = true;
     DataOutputStream sout = new DataOutputStream(System.out);   
     DataInputStream sin = new DataInputStream(System.in);   
     double result = BinomialOptions.calculate(sin, sout, s, k, t, v, rf, cp, n, chunk, start);
     if (result > 0.0)
       System.out.println(result);
  }
}
