num_bucketers = 2
num_mergers = 2
input_size = 200000000
jar_lib = [ref("http://www.cl.cam.ac.uk/~cs448/swterasort.jar")];
whole_input_ref = [ref("http://www.cl.cam.ac.uk/~cs448/input_all")];
input_refs = [ref("http://www.cl.cam.ac.uk/~cs448/input_0"), ref("http://www.cl.cam.ac.uk/~cs448/input_1")];

partition_outputs = spawn_exec("java", {"inputs":[whole_input_ref], "argv":[input_size, num_mergers], "class":"SWTeraSampler", "lib":jar_lib}, 1);

bucket_outputs = [];
for (i in range(0, num_bucketers)) {
    bucket_outputs[i] = spawn_exec("java", {"inputs": [partition_outputs[0], input_refs[i]], "argv":[num_mergers], "class":"SWTeraBucketer", "lib":jar_lib}, num_mergers);
}

merge_inputs = [];
for(i in range(0, num_mergers)) {
      merge_inputs[i] = [];
      for(j in range(0, num_bucketers)) {
      	    merge_inputs[i][j] = bucket_outputs[j][i];
      }
}

merge_outputs = [];
for(i in range(0, num_mergers)) {
      merge_outputs[i] = spawn_exec("java", {"inputs": [merge_inputs[i]], "argv":[num_bucketers], "class":"SWTeraMerger", "lib":jar_lib}, 1);
}

return merge_outputs;

 

