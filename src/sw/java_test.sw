
include "grab";

input_url = "file:///usr/share/dict/words";

inputs = [ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url)];
          
too_small_inputs = [ref(input_url)];
                    
test1_out = exec("java", {"inputs":inputs, "lib":[ref("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/JavaBindingsTests.jar")], "class":"Test1", "argv":["1", 234, 23]}, 2);
test2_out = exec("java", {"inputs":inputs, "lib":[ref("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/JavaBindingsTests.jar")], "class":"testpackage.Test2", "argv":["1", 234, 23]}, 2);

master_jar = ref("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/Master.jar");
worker_jar = ref("file:///local/scratch/dgm36/eclipse/workspace/mercator.hg/src/java/tests/Worker.jar");

test3_out = exec("java", {"inputs":inputs, "lib":[master_jar, worker_jar], "class":"Test3Master", "argv":["1", 234, 23]}, 2);

return test3_out;