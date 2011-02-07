
function java(class_name, input_refs, argv, jar_refs, num_outputs) {
	return spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv}, num_outputs);
}  

url = package("dataref");
input_refs = *url;

num_mappers = len(input_refs);
num_reducers = int(env["NUM_REDUCERS"]);

// Java code
jar_lib = [package("wordcount-jar"), package("grep-jar")];

// -----------------------------------------

// Map stage
map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = java("skywriting.examples.wordcount.WordCountMapper", [input_refs[i]], [], jar_lib, num_reducers);
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
      reduce_outputs[i] = java("skywriting.examples.wordcount.WordCountReducer", reduce_inputs[i], [], jar_lib, 1);
}

all_reducers = [];
for(i in range(0, num_reducers)) {
      all_reducers = all_reducers + reduce_outputs[i];
}

// -----------------------------------------

return (*(spawn_exec("sync", {"inputs" : all_reducers}, 1)[0]))[0];


 

