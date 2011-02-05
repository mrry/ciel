num_bucketers = 10;
num_mergers = 10;
sample_records = 100000; // Sizes of sample_input_0...n must add to this
jar_lib = [ref("http://www.cl.cam.ac.uk/~cs448/swterasort.jar")];
sample_input_refs = [ref("http://www.cl.cam.ac.uk/~cs448/sample_input_0"), 
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_1"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_2"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_3"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_4"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_5"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_6"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_7"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_8"),
		     ref("http://www.cl.cam.ac.uk/~cs448/sample_input_9")];
		     
// Until Skywriting gets input-slicing as a primitive
input_refs = [ref("http://www.cl.cam.ac.uk/~cs448/input_0"), 
	      ref("http://www.cl.cam.ac.uk/~cs448/input_1"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_2"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_3"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_4"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_5"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_6"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_7"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_8"),
	      ref("http://www.cl.cam.ac.uk/~cs448/input_9")];

prebucket_outputs = [];
for (i in range(0, num_bucketers)) {
    prebucket_outputs[i] = spawn_exec("java", {"inputs": [sample_input_refs[i]], "argv":[], "class":"SWTeraPreBucketer", "lib":jar_lib}, 1)[0];
}

partition_outputs = spawn_exec("java", {"inputs":prebucket_outputs, "argv":[num_bucketers, num_mergers, sample_records], "class":"SWTeraSampler", "lib":jar_lib}, 1);

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
      merge_outputs[i] = spawn_exec("java", {"inputs": merge_inputs[i], "argv":[num_bucketers], "class":"SWTeraMerger", "lib":jar_lib}, 1);
}

return merge_outputs;

 

