num_mappers = 4;
num_reducers = 2;
//sample_records = 100000; // Sizes of sample_input_0...n must add to this

function grab(url) {
   return *(exec("grab", {"urls":[url], "version":0}, 1)[0]);
}

jar_lib = [grab("http://www.cl.cam.ac.uk/~ms705/swwordcount.jar")];
     
// Until Skywriting gets input-slicing as a primitive
input_refs = [grab("http://www.cl.cam.ac.uk/~ms705/sw/wc_input_0"), 
	      grab("http://www.cl.cam.ac.uk/~ms705/sw/wc_input_1"),
	      grab("http://www.cl.cam.ac.uk/~ms705/sw/wc_input_2"),
	      grab("http://www.cl.cam.ac.uk/~ms705/sw/wc_input_3")];

map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = spawn_exec("java", {"inputs": [input_refs[i]], "argv":[], "class":"WordCountMapper", "lib":jar_lib}, num_reducers);
}

reduce_inputs = [];
for(i in range(0, num_reducers)) {
      reduce_inputs[i] = [];
      for(j in range(0, num_mappers)) {
      	    reduce_inputs[i][j] = map_outputs[j][i];
      }
}

reduce_outputs = [];
for(i in range(0, num_reducers)) {
      reduce_outputs[i] = spawn_exec("java", {"inputs": reduce_inputs[i], "argv":[], "class":"WordCountReducer", "lib":jar_lib}, 1);
}

ignore = *(spawn_exec("sync", {"inputs" : reduce_outputs}, 1)[0]);

while (!ignore) { foo = 1; }

return reduce_outputs;

 

