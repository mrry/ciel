source = spawn_exec("stdinout", {"inputs":[], "command_line":["cat", "/usr/share/dict/words"], "stream_output":true}, 1);

mid = spawn_exec("stdinout", {"inputs":source, "command_line":["head"]}, 1);

wc = spawn_exec("stdinout", {"inputs":mid, "command_line":["wc", "-w"]}, 1);

return *(wc[0]);