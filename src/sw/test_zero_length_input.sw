
include "grab";

producer_jar_ref = ref("file:///home/chris/skywriting/examples/tests/src/java/JitteryProducer.jar");                 
consumer_jar_ref = ref("file:///home/chris/skywriting/examples/tests/src/java/JitteryConsumer.jar");                 

producer_out = spawn_exec("java", {"inputs":[], "lib":[producer_jar_ref], "class":"tests.JitteryProducer", "argv":["0"], "stream_output": true}, 11);

consumer_in = [];
for(i in range(10)) {
      consumer_in += producer_out[i];
}

consumer_out = spawn_exec("java", {"inputs":consumer_in, "lib":[consumer_jar_ref], "class":"tests.JitteryConsumer", "argv":[], "debug_options": ["go_slow"]}, 1);

return [*(consumer_out[0]), *(producer_out[10])];