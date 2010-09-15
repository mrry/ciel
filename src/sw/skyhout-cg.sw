include "grab";
include "mapreduce";
include "java";

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

function matrix_vector (vector) {
	return lambda chunk: java("skywriting.examples.skyhout.linalg.MatrixVectorMultiplyTask",
							  [chunk, vector],
							  [],
							  jar_lib,
							  1);
}


function first_cg_iteration(matrix_a, b, rank_a, epsilon) {

	function first_cg_update(vector_chunks) {
		result = java("skywriting.examples.skyhout.linalg.ConjugateGradientReduceTask",
					  vector_chunks + [b],
					  [rank_a, true, epsilon],
					  jar_lib,
					  4);
					  
		return {"converged" : *result[0],
		        "x" : result[1],
		        "r" : result[2],
		        "p" : result[3]}; 
	}
	
	return mapreduce(matrix_a, matrix_vector(b), first_cg_update, 1)[0]; 

}

function cg_iteration(matrix_a, x, p, b) {

	function first_cg_update(vector_chunks) {
		result = java("skywriting.examples.skyhout.linalg.ConjugateGradientReduceTask",
					  vector_chunks + [x, p, b],
					  [rank_a, true, epsilon],
					  jar_lib,
					  4);
					  
		return {"converged" : *result[0],
		        "x" : result[1],
		        "r" : result[2],
		        "p" : result[3]}; 
	}
	
	return mapreduce(matrix_a, matrix_vector(b), cg_update, 1)[0]; 

}

input = [];

result = first_cg_iteration(input, b);

coverged = result["converged"];
while (!converged) {
	r = result["r"];
	p = result["p"];

	result = cg_iteration(input, x, r, p);

	converged = result["converged"];
	x = result["x"];
}

return x;

while (!converged) {
	result = kmeans_iteration(input, old_clusters, convergence_delta, r);
	converged = result["converged"];
	old_clusters = result["clusters"]; 
}

return old_clusters;