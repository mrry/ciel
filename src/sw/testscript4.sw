
include "grab";

foo = function (file_ref) {
	return *(exec("stdinout", {"inputs": [file_ref], "command_line": ["wc", "-w"]}, 1)[0]);
};

input_url = "file:///usr/share/dict/words";

inputs = [ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url),
          ref(input_url)];
          
mid = [];
for (i in range(0, len(inputs))) {
	mid[i] = *spawn(foo, [inputs[i]]);
}

return mid;