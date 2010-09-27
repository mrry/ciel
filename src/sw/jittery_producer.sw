
jar_ref = ref("file:///home/chris/skywriting/examples/tests/src/java/tests/JitteryProducer.jar");                 

producer_out = spawn_exec("java", {"inputs":[], "lib":[jar_ref], "class":"tests.JitteryProducer", "argv":[], "stream_output": true}, 1);

consumer_out = spawn_exec("stdinout", {"inputs":producer_out, "command_line":["wc", "-c"]}, 1);

return *(consumer_out[0]);