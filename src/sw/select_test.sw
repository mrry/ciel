function sleeper (sleep_time) {
	ignore = exec("stdinout", {"inputs":[], "command_line":["/bin/sleep", sleep_time]}, 1);
	return sleep_time;
}

a = spawn(sleeper, [1]);
b = spawn(sleeper, [1]);
c = spawn(sleeper, [1]);

return select([a, b, c]);