// Library functions
include "grab";
//include "java";

function java(class_name, input_refs, argv, jar_refs, num_outputs) {
	 return spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv}, num_outputs);
}  

// Paste the reference returned by sw-load here
url = package("dataref");
input_refs = *url;

// Configuration
num_mappers = len(input_refs);
num_reducers = 2;

// Change the regexp here
regexp = "for";

jar_lib = [package("grep_jar")];

// -----------------------------------------

// Map stage
map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = java("skywriting.examples.grep.GrepMapper", [input_refs[i]], [regexp], jar_lib, num_reducers);
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
      reduce_outputs[i] = java("skywriting.examples.grep.GrepReducer1", reduce_inputs[i], [], jar_lib, 1)[0];
}

// Reduce stage 2
reduce_outputs2 = java("skywriting.examples.grep.GrepReducer2", reduce_outputs, [], jar_lib, 1);

// -----------------------------------------

return (*(spawn_exec("sync", {"inputs" : reduce_outputs2}, 1)[0]))[0];
 

