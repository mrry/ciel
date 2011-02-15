include "mapreduce";
//include "java";

function java(class_name, input_refs, argv, jar_refs, num_outputs, sweetheart) {
        return spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv, "eager_fetch" : true, "make_sweetheart" : sweetheart}, num_outputs);
}  

jar_lib = [package("skyhout"), package("mahout-core"), package("mahout-math"), package("mahout-coll"), 
	   package("logging"), package("slf4j-api"), package("slf4j-jcl"), package("uc-maths"), 
	   package("gson"), package("hadoop-core")];

function kmeans_iteration(data_chunks, old_clusters, convergenceDelta, num_reducers) {

	kmeans_map = function(input_chunk) {
		return java("skywriting.examples.skyhout.kmeans.KMeansMapTask",
		            [input_chunk, old_clusters],
		            [],
		            jar_lib,
		            num_reducers,
			    input_chunk);
	};
	
	kmeans_reduce = function(reduce_input) {
		result = java("skywriting.examples.skyhout.kmeans.KMeansReduceTask",
		            reduce_input + [old_clusters],
		            [convergenceDelta],
		            jar_lib,
		            2,
			    []);
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

input_vectors = *package("vector-input");

input_clusters = (*package("cluster-input"))[0];

convergence_delta = "0.00000001";

r = 1;
i = 0;

old_clusters = input_clusters;
converged = false;
while ((i < 5) && !converged) {
	result = kmeans_iteration(input_vectors, old_clusters, convergence_delta, r);
	converged = result["converged"];
	old_clusters = result["clusters"];
	i = i + 1; 
}

return old_clusters;