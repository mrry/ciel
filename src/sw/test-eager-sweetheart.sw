include "grab";

foo = function (file_ref) {
	return *(spawn_exec("stdinout", {"inputs": [file_ref], "command_line": ["wc", "-w"], "make_sweetheart": file_ref, "eager_fetch": true}, 1)[0]);
};

input_url = "file:///usr/share/dict/words";

inputs = [grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url),
          grab(input_url)];
          
mid = [];
for (i in range(0, len(inputs))) {
	mid[i] = *spawn(foo, [inputs[i]]);
}

return mid;