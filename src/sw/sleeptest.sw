function recurse_spawner (current, limit) {
	foo = exec("stdinout", {"inputs": [], "command_line": ["/bin/sleep", "5"]}, 1);

	if (current == limit) {
	    return 1;
	} else {
	 	left = *spawn(recurse_spawner, [current + 1, limit]);
	 	right = *spawn(recurse_spawner, [current + 1, limit]);
	 	return left + right;
	} 
}

return recurse_spawner(0, 8);
