
num_mappers = int(env["NUM_MAPPERS"]);
num_samples = int(env["NUM_SAMPLES"]);
jar_lib = [package("pi-jar")];

map_outputs = [];
offset = 0;
for (i in range(0, num_mappers)) {
    map_outputs = map_outputs + spawn_other("java", {"args":{"inputs":[], "argv":[num_samples, offset], "class":"skywriting.examples.pi.PiMapper", "lib":jar_lib}, "scheduling_class":"cpu", "scheduling_type":"pi-map", "n_outputs":1});
    offset = offset + num_samples;
}

reduce_output = spawn_other("java", {"args":{"inputs":map_outputs, "argv":[], "class":"skywriting.examples.pi.PiReducer", "lib":jar_lib}, "scheduling_class":"cpu", "scheduling_type":"pi-reduce", "n_outputs":1});

return *(exec("sync", {"inputs":reduce_output}, 1)[0]);
