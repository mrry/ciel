function recurse_spawner (current, limit) {
	if (current == limit) {
	    return 1;
	} else {
	 	left = *spawn(recurse_spawner, [current + 1, limit]);
	 	right = *spawn(recurse_spawner, [current + 1, limit]);
	 	return left + right;
	} 
}

return recurse_spawner(0, 17);