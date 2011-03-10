
num_mappers = 100;
num_samples = 10000000;
jar_lib = [package("pi-jar")];

map_outputs = [];
offset = 0;
for (i in range(0, num_mappers)) {
    map_outputs = map_outputs + spawn_exec("java", {"inputs":[], "argv":[num_samples, offset], "class":"skywriting.examples.pi.PiMapper", "lib":jar_lib}, 1);
    offset = offset + num_samples;
}

reduce_output = spawn_exec("java", {"inputs":map_outputs, "argv":[], "class":"skywriting.examples.pi.PiReducer", "lib":jar_lib}, 1);

return *(exec("sync", {"inputs":reduce_output}, 1)[0]);
