
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
                    
test1_out = exec("java", {"inputs":inputs, "lib":[package("testsjar")], "class":"Test1", "argv":["1", 234, 23]}, 2);
test2_out = exec("java", {"inputs":inputs, "lib":[package("testsjar")], "class":"testpackage.Test2", "argv":["1", 234, 23]}, 2);

master_jar = package("masterjar");
worker_jar = package("workerjar");

test3_out = exec("java", {"inputs":inputs, "lib":[master_jar, worker_jar], "class":"Test3Master", "argv":["1", 234, 23]}, 2);

return (*(exec("sync", {"inputs": [test1_out[0], test2_out[0], test3_out[0]]}, 1)[0]));
