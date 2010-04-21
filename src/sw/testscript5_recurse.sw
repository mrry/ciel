l = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];

function fold (list, current_pos, running_total, f) {
	if (current_pos >= len(list)) {
		return running_total;
	} else {
	    return fold(list, current_pos + 1, f(running_total, list[current_pos]), f);
	} 
}

return fold(l, 0, 0, lambda x, y: x + y);