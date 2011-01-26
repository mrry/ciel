include "grab";

num_mappers = 99;
num_samples = 1;
jar_lib = [grab("http://www.cl.cam.ac.uk/~dgm36/sky-eg-pi.jar")];

map_outputs = [];
offset = 0;
for (i in range(0, num_mappers)) {
    map_outputs = map_outputs + spawn_exec("java", {"inputs":[], "argv":[num_samples, offset], "class":"skywriting.examples.pi.PiMapper", "lib":jar_lib}, 1);
    offset = offset + num_samples;
}

reduce_output = exec("java", {"inputs":map_outputs, "argv":[], "class":"skywriting.examples.pi.PiReducer", "lib":jar_lib}, 1);
return reduce_output;
