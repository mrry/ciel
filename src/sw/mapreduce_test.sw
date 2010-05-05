function map(f, list) {
  outputs = [];
  for (i in range(len(list))) {
    outputs[i] = f(list)
  }
  return outputs;
}

function shuffle(inputs, num_outputs) {
  outputs = [];
    outputs[i] = []
    for (j in range(len(inputs))) {
      outputs[i][j] = inputs[j][i]
    }
  }
  return outputs;
}

function mapreduce(inputs, mapper, reducer, r) {
  map_outputs = map(mapper, inputs);
  reduce_inputs = shuffle(map_outputs);
  reduce_outputs = map(reducer, reduce_inputs);
  return reduce_outputs;
}

inputs = [ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part9"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part4"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part1"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part0"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part3"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part2"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part7"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part6"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part8"),
ref("file:///local/scratch/ms705/skywriting/hadoop-0.20-svn/bin/PiEstimator_TMP_3_141592654/in/part5")];

function make_hadoop_map_task(conf, num_reducers) {
  return function(i) {
    spawn_exec("java", {"inputs": [conf], "lib":["/usr/lib/hadoop/skywriting_bridge/"], "args":[i], "class":"SWMapEntryPoint"}, num_reducers);
  };
}

function make_hadoop_reduce_task(conf) {
  return function(i) {
    spawn_exec("java", {"inputs": [conf], "lib":["/usr/lib/hadoop/skywriting_bridge/"], "args":[i], "class":"SWReduceEntryPoint"}, 1);
  };
}

conf = ref(""swbs://skiing-0.xeno.cl.cam.ac.uk:9000/3"");

return map_reduce(range(0, 10), make_hadoop_map_task(conf, 1), make_hadoop_reduce_task(conf), 1);























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
