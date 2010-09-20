// Library functions
include "grab";
include "java"

num_mappers = 4;
num_reducers = 2;

// Input data - MODIFY THIS FOR EACH RUN
// Use sw-load to put input data into the cluster
// and paste the reference returned into here
input_refs = *ref("swbs://breakout-0.xeno.cl.cam.ac.uk:8001/upload:5b63bd0c-800a-4de7-9c08-6184ad6bca4e:index");

// Java code
jar_lib = [grab("http://www.cl.cam.ac.uk/~ms705/swwordcount.jar")];

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

 

