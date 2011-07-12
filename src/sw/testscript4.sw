
include "grab";

foo = function (file_ref) {
	return *(exec("stdinout", {"inputs": [file_ref], "command_line": ["wc", "-w"]}, 1)[0]);
};

inputs = [package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words"),
          package("words")];
          
mid = [];
for (i in range(0, len(inputs))) {
	mid[i] = *spawn(foo, [inputs[i]]);
}

return mid;