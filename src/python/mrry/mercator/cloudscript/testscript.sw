map = function (input, map_class, num_reducers) {
	return range(0, num_reducers);
};

reduce = function (reduce_input, reduce_class) {
	i = 0;
	for (elem in reduce_input) {
		i = i + elem;
	}
	return i;
};

map_reduce = function (map_inputs, map_class, reduce_class) {

	num_reducers = 10;

	map_outputs = [];
	i = 0;
	for (input in map_inputs) {
		map_outputs[i] = map(input, map_class, num_reducers);
		i = i + 1;
	}

	reduce_inputs = [];
	
	reduce_outputs = [];
	
	for (i in range(0, num_reducers)) {
		reduce_inputs[i] = [];
		for (j in range(0, len(map_outputs))) {
			reduce_inputs[i][j] = map_outputs[j][i];
		}
		reduce_outputs[i] = reduce(reduce_inputs[i], reduce_class);	
	}
	
	return reduce_outputs;

};

results = map_reduce([1, 2, 3, 4], "uk.co.mrry.mercator.MapTest", "uk.co.mrry.mercator.ReduceTest");

testy = function (x) { return 100 + x; } (50);

lesty = (lambda x, y : 100 + testy + y)(50, 3);

hfunc = lambda x : (lambda y : x + y);

h = hfunc(10);

z = [h(20), h(30), h(40)];
