// Library functions
include "grab";
include "stdinout";
include "environ";

// Paste the reference returned by sw-load here
// MODIFY THIS FOR EACH RUN
input_refs = *ref("swbs://skirmish-0.xeno.cl.cam.ac.uk:8001/upload:a6b959af-52e8-4fb9-ac1b-ec0900954f08:index");

// Numbers of mappers and reducers
num_mappers = len(input_refs);
num_reducers = 1;

// -----------------------------------------

map_outputs = [];
for (i in range(0, num_mappers)) {
    map_outputs[i] = stdinout([input_refs[i]], ["/usr/bin/worker", "--stdin"]);
}

reduce_outputs = [];
for(i in range(0, num_reducers)) {
      reduce_outputs[i] = environ(map_outputs, ["/usr/bin/reduce.sh"], 1);
}

// -----------------------------------------

ignore = *(spawn_exec("sync", {"inputs" : reduce_outputs}, 1)[0]);

while (!ignore) { foo = 1; }

return reduce_outputs;

 

