single_map = function (map_input, map_class, num_reducers) {
	return range(0, num_reducers);
};

map = function (map_inputs, map_class, num_reducers) {
	map_outputs = [];
	for (i in range(0, len(map_inputs))) {
		map_outputs[i] = single_map(map_inputs[i], map_class, num_reducers);
		i = i + 1;
	}
	return map_outputs;
};

shuffle = function (map_outputs, num_reducers) {
	reduce_inputs = [];
	for (i in range(0, num_reducers)) {
		reduce_inputs[i] = [];
		for (j in range(0, len(map_outputs))) {
			reduce_inputs[i][j] = map_outputs[j][i];
		}
	}
	return reduce_inputs;
};

single_reduce = function (reduce_input, reduce_class) {
	i = 0;
	for (elem in reduce_input) {
		i = i + elem;
	}
	return i;
};

reduce = function (reduce_inputs, reduce_class) {
	reduce_outputs = [];
	for (i in range(0, len(reduce_inputs))) {
		reduce_outputs[i] = single_reduce(reduce_inputs[i], reduce_class);
	}
	return reduce_outputs;
};

map_reduce = function (map_inputs, map_class, reduce_class) {
	num_reducers = 10;
	map_outputs = map(map_inputs, map_class, num_reducers);
	reduce_inputs = shuffle(map_outputs, num_reducers);
	reduce_outputs = reduce(reduce_inputs, reduce_class);
	return reduce_outputs;
};

results = map_reduce([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "uk.co.mrry.mercator.MapTest", "uk.co.mrry.mercator.ReduceTest");
