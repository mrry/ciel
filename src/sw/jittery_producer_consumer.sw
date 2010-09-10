
producer_jar_ref = ref("file:///home/chris/skywriting/src/java/tests/JitteryProducer.jar");                 
consumer_jar_ref = ref("file:///home/chris/skywriting/src/java/tests/JitteryConsumer.jar");                 

producer_out = spawn_exec("java", {"inputs":[], "lib":[producer_jar_ref], "class":"JitteryProducer", "argv":[], "stream_output": true}, 10);

consumer_out = spawn_exec("java", {"inputs":producer_out, "lib":[consumer_jar_ref], "class":"JitteryConsumer", "argv":[]}, 1);

return *(consumer_out[0]);