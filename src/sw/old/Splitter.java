import uk.co.mrry.mercator.task.Task;
import org.json.simple.*;
import java.io.*;

public class Splitter implements Task {
    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
       System.out.println("splitter start");
       try {
           BufferedReader psbr = new BufferedReader(new InputStreamReader(fis[1]));
           String line = psbr.readLine();
           JSONArray box = (JSONArray)JSONValue.parse(line);
           JSONArray p1 = (JSONArray)box.get(0);
           JSONArray p2 = (JSONArray)box.get(1);
           float bx1 = ((Number)p1.get(0)).floatValue();
           float by1 = ((Number)p1.get(1)).floatValue();
           float bx2 = ((Number)p2.get(0)).floatValue();
           float by2 = ((Number)p2.get(1)).floatValue();
           float bxmid = (bx1 + bx2) / 2;
           float bymid = (by1 + by2) / 2;
           Writer[] splits = new Writer[4];
           for (int i=0; i<4; i++) {
              splits[i] = new BufferedWriter(new OutputStreamWriter(fos[i]));
           }
           
           psbr = new BufferedReader(new InputStreamReader(fis[0]));
           int count = 0;
           float cx=0.0f, cy =0.0f;
           while ( (line=psbr.readLine()) != null) {
               count ++;
               JSONArray p = (JSONArray)JSONValue.parse(line);
               float x = ((Number)p.get(0)).floatValue();
               float y = ((Number)p.get(1)).floatValue();
               cx += x;
               cy += y;
               int index = -1;
               if (x <= bxmid && y <= bymid)
                  index = 0;
               else if (x <= bxmid && y > bymid)
                  index = 1;
               else if (x > bxmid && y <= bymid)
                  index = 2;
               else if (x > bxmid && y > bymid)
                  index = 3;
               splits[index].write(line);
               splits[index].write("\n");
           }
           for (int i=0; i<4; i++) {
              splits[i].close();
           }
           cx /= count;
           cy /= count;

           System.out.printf("My inputs begin with: %c, %c\n", (char)fis[0].read(), (char)fis[1].read());
           
           PrintWriter pw;
           pw = new PrintWriter(new OutputStreamWriter(fos[4]));
           pw.printf("[[%f,%f],[%f,%f]]\n", bx1, by1, bxmid, bymid);
           pw.close();

           pw = new PrintWriter(new OutputStreamWriter(fos[5]));
           pw.printf("[[%f,%f],[%f,%f]]\n", bx1, bymid, bxmid, by2);
           pw.close();

           pw = new PrintWriter(new OutputStreamWriter(fos[6]));
           pw.printf("[[%f,%f],[%f,%f]]\n", bxmid, by1, bx2, bymid);
           pw.close();

           pw = new PrintWriter(new OutputStreamWriter(fos[7]));
           pw.printf("[[%f,%f],[%f,%f]]\n", bxmid, bymid, bx2, by2);
           pw.close();

           pw = new PrintWriter(new OutputStreamWriter(fos[8]));
           pw.printf("{ \"centroid\" : [ %f,%f ], \"count\": %d }", cx, cy, count);
           pw.close();
       }
       catch(IOException e) {
            System.out.println("IOException using my inputs or outputs: " + e.toString());
       } 
    }
}
