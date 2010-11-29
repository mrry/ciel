
import skypy

def shuffle(inputs, num_outputs):
    return [[inputs[j][i] for j in len(inputs)] for i in range(num_outputs)]

def mapreduce(inputs, mapper, reducer, r):
    map_outputs = map(mapper, inputs)
    reduce_inputs = shuffle(map_outputs, r)
    return map(reducer, reduce_inputs)

def grab(url):
    ref = (skypy.spawn_exec("grab", {"urls": [url], "version": 0}, 1))[0]
    return (skypy.deref_json(ref))

def java(class_name, input_refs, argv, jar_refs, num_outputs):
    return skypy.spawn_exec("java", {"inputs" : input_refs, "class" : class_name, "lib" : jar_refs, "argv" : argv}, num_outputs)

convergence_delta = "0.00000001"

def skypy_main(vector_in, cluster_in):
    jar_lib = map(grab, 
                  ["http://www.cl.cam.ac.uk/~dgm36/skyhout.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/mahout-core-0.3.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/mahout-math-0.3.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/mahout-collections-0.3.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/commons-logging-1.1.1.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/slf4j-api-1.5.8.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/slf4j-jcl-1.5.8.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/uncommons-maths-1.2.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/gson-1.3.jar",
                   "http://www.cl.cam.ac.uk/~dgm36/hadoop-core-0.20.2.jar"])

    def kmeans_iteration(data_chunks, old_clusters, convergenceDelta, num_reducers):

	def kmeans_map(input_chunk):
            return java("skywriting.examples.skyhout.kmeans.KMeansMapTask",
                        [input_chunk, old_clusters],
                        [],
                        jar_lib,
                        num_reducers)
	
	def kmeans_reduce(reduce_input):
            result = java("skywriting.examples.skyhout.kmeans.KMeansReduceTask",
                          reduce_input + [old_clusters],
                          [convergenceDelta],
                          jar_lib,
                          2)
            return {"cluster" : result[0], "converged" : skypy.deref(result[1])}
	
	new_clusters_and_decisions = mapreduce(data_chunks, kmeans_map, kmeans_reduce, num_reducers)

	new_clusters = [x["decision"] for x in new_clusters_and_decisions]
        converged = True
        for x in new_clusters_and_decisions:
            if not x["converged"]:
                converged = False
                break

	return {"converged" : converged, "clusters" : new_clusters[0]}

    # TODO: Something about this
    input_vectors = skypy.deref_json(grab(vector_in))
    input_clusters = (skypy.deref_json(grab(cluster_in)))[0]

    r = 1
    i = 0

    old_clusters = input_clusters
    converged = False
    while i < 10 and not converged:
        result = kmeans_iteration(input_vectors, old_clusters, convergence_delta, r)
        converged = result["converged"]
        old_clusters = result["clusters"]
	i += 1

    return old_clusters


