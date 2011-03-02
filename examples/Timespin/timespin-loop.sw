include "mapreduce";

function time_spin (t, n, j) {
	rets = [];
	
	for (i in range(n)) {
		rets += (spawn_exec("stdinout", {"inputs": [], "command_line": ["timespin", t], "foo" : env["FOO"], "bar" : i, "round" : j}, 1)[0]);
	}

	mrets = map(lambda x : *x, rets);

	ret = true;

	for (m in mrets) {
		ret = ret && m;
	}

	return ret;
}


for (k in range(3)) {
	boo = time_spin(10, 120, k);
}

return boo;
