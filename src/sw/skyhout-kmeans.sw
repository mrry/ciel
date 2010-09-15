include "grab";
include "mapreduce";

jar_lib = [grab("http://www.cl.cam.ac.uk/~dgm36/skyhout.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-core-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-math-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/mahout-collections-0.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/commons-logging-1.1.1.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-api-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/slf4j-jcl-1.5.8.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/uncommons-maths-1.2.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/gson-1.3.jar"),
           grab("http://www.cl.cam.ac.uk/~dgm36/hadoop-core-0.20.2.jar")];

function kmeans_iteration(data_chunks, old_clusters, convergenceDelta, num_reducers) {

	kmeans_map = function(input_chunk) {
		return spawn_exec("java", {"inputs" : [input_chunk, old_clusters], "lib" : jar_lib, "class" : "skywriting.examples.skyhout.kmeans.KMeansMapTask", "args" : []}, num_reducers);
	};
	
	kmeans_reduce = function(reduce_input) {
		result = spawn_exec("java", {"inputs" : reduce_input + [old_clusters], "lib" : jar_lib, "class" : "skywriting.examples.skyhout.kmeans.KMeansReduceTask", "argv" : [convergenceDelta]}, 2);
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
num_maps = 1;
input = spawn_exec("java", {"inputs" : [input_text], "lib" : jar_lib, "class" : "skywriting.examples.skyhout.input.VectorInputParserTask"}, num_maps);
 
k = 10;

init_clusters = spawn_exec("java", {"inputs" : input, "lib" : jar_lib, "class" : "skywriting.examples.skyhout.kmeans.KMeansSeedGenerator", "argv" : [k]}, 1)[0];

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