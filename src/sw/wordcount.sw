// Library functions
include "grab";
//include "java";

function java(class_name, input_refs, argv, jar_refs, num_outputs) {
   f = env["FOO"];
	return spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv, "foo" : f}, num_outputs);
}  

// Input data - MODIFY THIS FOR EACH RUN
// Use sw-load to put input data into the cluster
// and paste the reference returned into here
url = env["DATA_REF"];
input_refs = *grab(url);


num_mappers = len(input_refs);
num_reducers = env["NUM_REDUCERS"];

// Java code
jar_lib = [grab("http://www.cl.cam.ac.uk/~ms705/sky-eg-wordcount.jar")];

// -----------------------------------------

// Map stage
map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = java("WordCountMapper", [input_refs[i]], [], jar_lib, num_reducers);
}

// Shuffle stage
reduce_inputs = [];
for(i in range(0, num_reducers)) {
      reduce_inputs[i] = [];
      for(j in range(0, num_mappers)) {
      	    reduce_inputs[i][j] = map_outputs[j][i];
      }
}

// Reduce stage
reduce_outputs = [];
for(i in range(0, num_reducers)) {
      reduce_outputs[i] = java("WordCountReducer", reduce_inputs[i], [], jar_lib, 1);
}

// -----------------------------------------

ignore = *(spawn_exec("sync", {"inputs" : reduce_outputs}, 1)[0]);

while (!ignore) { foo = 1; }

return reduce_outputs;

 

