java_lib = [ref("http://www.cl.cam.ac.uk/~dgm36/kmeans.jar")];

points = spawn_exec("java", {"inputs":[], "lib":java_lib, "class":"tests.kmeans.KMeansInitialPoints", "argv":[1000, 10]}, 1);

centroids = spawn_exec("java", {"inputs":[], "lib":java_lib, "class":"tests.kmeans.KMeansInitialPoints", "argv":[10, 10]}, 1);

map_output = spawn_exec("java", {"inputs":[points[0], centroids[0]], "lib":java_lib, "class":"tests.kmeans.KMeansMap", "argv":[10, 10]}, 1);

reduce_output = spawn_exec("java", {"inputs":[map_output[0]], "lib":java_lib, "class":"tests.kmeans.KMeansReduce", "argv":[10, 10]}, 1);

return reduce_output;