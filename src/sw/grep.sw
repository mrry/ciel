// Library functions
include "grab";
include "java"

// Configuration
num_mappers = len(input_refs);
num_reducers = 2;

// Paste the reference returned by sw-load here
input_refs = *ref("swbs://heavenly.cl.cam.ac.uk:8081/upload:37a7814c-e1f3-433d-9fdd-f3f3974c0129:index");

// Change the regexp here
regexp = "(Derek|Malte|Steve|Chris|Anil|Steven)";

jar_lib = [grab("http://www.cl.cam.ac.uk/~ms705/swgrep.jar")];

// -----------------------------------------

// Map stage
map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = java("GrepMapper", [input_refs[i]], [regexp], jar_lib, num_reducers);
}

// Shuffle stage
reduce_inputs = [];
for(i in range(0, num_reducers)) {
      reduce_inputs[i] = [];
      for(j in range(0, num_mappers)) {
      	    reduce_inputs[i][j] = map_outputs[j][i];
      }
}

// Reduce stage 1
reduce_outputs = [];
for(i in range(0, num_reducers)) {
      reduce_outputs[i] = java("GrepReducer1", reduce_inputs[i], [], jar_lib, 1)[0];
}

// Reduce stage 2
reduce_outputs2 = java("GrepReducer2", reduce_outputs, [], jar_lib, 1);

// -----------------------------------------

ignore = *(spawn_exec("sync", {"inputs" : reduce_outputs2}, 1)[0]);

while (!ignore) { foo = 1; }

return reduce_outputs2[0];

 

