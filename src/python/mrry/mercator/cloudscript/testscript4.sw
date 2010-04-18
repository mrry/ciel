foo = function (file_ref) {
	return exec("stdinout", {"inputs": [file_ref], "command_line": ["wc", "-w"]}, 1);
};

inputs = [ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1"),
          ref("file:///usr/share/man/man1/zsh.1")];
          
mid = [];
for (i in range(0, len(inputs))) {
	mid[i] = *spawn(foo, [inputs[i]]);
}

return mid;