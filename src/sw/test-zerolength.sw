jar = *(exec("grab", {"urls":["http://www.cl.cam.ac.uk/~dgm36/test.jar"], "version":0}, 1)[0]);

prod = spawn_exec("stdinout", {"command_line":["sleep", "10"], "inputs":[], "stream_output":true}, 1);

cons = spawn_exec("java", {"argv":["foo", "bar"], "lib":[jar], "inputs":prod, "class":"tests.Test1"}, 1);

return *((spawn_exec("sync", {"inputs":cons}, 1))[0]);
