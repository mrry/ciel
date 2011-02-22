include "grab";
include "mapreduce";

function environ(input_refs, cmd_line, num_outputs, env, sweetheart) {
	 f = 3;
	return spawn_exec("environ", {"inputs" : input_refs, "command_line" : cmd_line, "eager_fetch" : true, "make_sweetheart" : sweetheart, "env" : env, "foo" : f}, num_outputs);
}

function stdinout(input_refs, cmd_line, sweetheart) {
	 f = 3;
	return spawn_exec("stdinout", {"inputs" : input_refs, "command_line" : cmd_line, "eager_fetch" : true, "make_sweetheart" : sweetheart, "foo" : f}, 1)[0];
}

function lr_iteration(data_chunks, old_w, convergence_delta) {

	 lr_map = function(input_chunk) {
	 	return environ([input_chunk, old_w], ["/local/scratch/dgm36/skywriting/skywriting/examples/LogisticRegression/lr_map_environ.sh"], 1, {"LR_MAP_EXEC" : "/local/scratch/dgm36/skywriting/skywriting/examples/LogisticRegression/lr-map", "DIM" : "10", "PARSE" : "1"}, input_chunk);
	 };

	 lr_reduce = function(part_vecs) {
	 	   return stdinout([old_w] + part_vecs, ["/local/scratch/dgm36/skywriting/skywriting/examples/LogisticRegression/lr-reduce", "10"], []);
	 };

	 new_w = mapreduce(data_chunks, lr_map, lr_reduce, 1);

	 return {"vector" : new_w, "converged" : false};

}

input_vectors = *grab(env["DATA_INPUT_URL"]);

input_clusters = stdinout([], ["/local/scratch/dgm36/skywriting/skywriting/examples/LogisticRegression/lr-reduce", "10", "1"], []);

convergence_delta = "0.00000001";

i = 0;

old_clusters = input_clusters;
converged = false;
while ((i < 100) && !converged) {
	result = lr_iteration(input_vectors, old_clusters, convergence_delta);
	converged = result["converged"];
	old_clusters = result["vector"][0];
	i = i + 1;
}

return *(old_clusters);