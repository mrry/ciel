// Helper function to grab URL references
function grab(url) {
   return *(exec("grab", {"urls":[url], "version":0}, 1)[0]);
}

jar_lib = [grab("http://www.cl.cam.ac.uk/~dgm36/skyhout.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-core-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-math-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-collections-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/commons-logging-1.1.1.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-api-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-jcl-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/uncommons-maths-1.2.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/hadoop-core-0.20.2.jar")];

function map(f, list) {
  outputs = [];
  for (i in range(len(list))) {
    outputs[i] = f(list[i]);
  }
  return outputs;
}

function shuffle(inputs, num_outputs) {
  outputs = [];
  for (i in range(num_outputs)) {
    outputs[i] = [];
    for (j in range(len(inputs))) {
      outputs[i][j] = inputs[j][i];
    }
  }
  return outputs;
}

function mapreduce(inputs, mapper, reducer, r) {
  map_outputs = map(mapper, inputs);
  reduce_inputs = shuffle(map_outputs, r);
  reduce_outputs = map(reducer, reduce_inputs);
  return reduce_outputs;
}

function kmeans_iteration(data_chunks, old_clusters, convergenceDelta, num_reducers) {

	kmeans_map = function(input_chunk) {
		return spawn_exec("java", {"inputs" : [input_chunk, old_clusters], "lib" : jar_lib, "class" : "skyhout.kmeans.KMeansMapTask", "args" : []}, num_reducers);
	};
	
	kmeans_reduce = function(reduce_input) {
		result = spawn_exec("java", {"inputs" : reduce_input + [old_clusters], "lib" : jar_lib, "class" : "skyhout.kmeans.KMeansReduceTask", "argv" : [convergenceDelta]}, 2);
		return {"cluster" : result[0], "converged" : *(result[1])}; 
	};
	
	new_clusters_and_decisions = mapreduce(data_chunks, kmeans_map, kmeans_reduce, num_reducers);

	new_clusters = [];

	converged = true;
	for (new_cluster_and_decision in new_clusters_and_decisions) {
		new_clusters += new_cluster_and_decision["cluster"];
		converged = converged && new_cluster_and_decision["converged"]; 
	} 

	return {"converged" : converged, "clusters" : new_clusters[0]};
}

input_text = grab("http://archive.ics.uci.edu/ml/databases/synthetic_control/synthetic_control.data");
num_maps = 10;
input = spawn_exec("java", {"inputs" : [input_text], "lib" : jar_lib, "class" : "skyhout.input.VectorInputParserTask"}, num_maps);
 
k = 2;

init_clusters = spawn_exec("java", {"inputs" : input, "lib" : jar_lib, "class" : "skyhout.kmeans.KMeansSeedGenerator", "argv" : [k]}, 1)[0];

measure_class = "";

convergence_delta = "0.5";

r = 1;

old_clusters = init_clusters;
converged = false;
while (!converged) {
	result = kmeans_iteration(input, old_clusters, convergence_delta, r);
	converged = result["converged"];
	old_clusters = result["clusters"]; 
}

return old_clusters;