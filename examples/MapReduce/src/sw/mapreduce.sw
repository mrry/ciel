include "mapreduce";
include "environ";
include "stdinout";
include "sync";

function make_environ_map_task(num_reducers) {
    return function(map_input) {
        return environ([map_input], [package("map-bin")], num_reducers);
    };
}

function make_environ_reduce_task() {
    return function(reduce_inputs) {
        return environ(reduce_inputs, [package("reduce-bin")], 1)[0];
    };
}

inputs = *package("input-files");
num_reducers = int(env["NUM_REDUCERS"]);

results = mapreduce(inputs, make_environ_map_task(num_reducers), make_environ_reduce_task(), num_reducers);

catted_results = stdinout(results, ["/bin/cat"]);

return sync([catted_results]);

