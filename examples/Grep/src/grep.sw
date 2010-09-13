num_reducers = 2;

// Helper function to grab URL references
function grab(url) {
   return *(exec("grab", {"urls":[url], "version":0}, 1)[0]);
}

jar_lib = [grab("http://www.cl.cam.ac.uk/~ms705/swgrep.jar")];
     
// Paste the reference returned by sw-load here
input_refs = *ref("swbs://lochain-0.xeno.cl.cam.ac.uk:8001/upload:99ab4e58-96a6-491b-98e1-edc21e949c75:index");

num_mappers = len(input_refs);
//num_mappers = 4;

map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = spawn_exec("java", {"inputs": [input_refs[i]], "argv":["Steve"], "class":"GrepMapper", "lib":jar_lib}, num_reducers);
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
      reduce_outputs[i] = spawn_exec("java", {"inputs": reduce_inputs[i], "argv":[], "class":"GrepReducer1", "lib":jar_lib}, 1)[0];
}

reduce_outputs2 = spawn_exec("java", {"inputs": reduce_outputs, "argv":[], "class":"GrepReducer2", "lib":jar_lib}, 1);

ignore = *(spawn_exec("sync", {"inputs" : reduce_outputs2}, 1)[0]);

while (!ignore) { foo = 1; }

return reduce_outputs2[0];

 

