include "mapreduce";

function time_spin (t, n) {
	rets = [];
	
	for (i in range(n)) {
		rets += (spawn_exec("stdinout", {"inputs": [], "command_line": ["timespin", t], "foo" : env["FOO"], "bar" : i}, 1)[0]);
	}

	return map(lambda x : *x, rets);
}

return time_spin(env["SPIN_TIME"], int(env["SPIN_DEGREE"]));
