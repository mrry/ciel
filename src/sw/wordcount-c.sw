// Library functions
include "grab";
#include "stdinout";
include "environ";

function stdinout(input_refs, cmd_line) {
   f = env["FOO"];
	return spawn_exec("stdinout", {"inputs" : input_refs, "command_line" : cmd_line, "foo" : f }, 1)[0];
}

// Paste the reference returned by sw-load here 
// MODIFY THIS FOR EACH RUN
url = env["DATA_REF"];
input_refs = *grab(url);

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

return (*(spawn_exec("sync", {"inputs" : reduce_outputs}, 1)[0]))[0];
 

