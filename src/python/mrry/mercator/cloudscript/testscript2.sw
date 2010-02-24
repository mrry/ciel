i = 11;
j = i + 1;

foo = function (x) {
	return 100 + j + x;
};

do {
	j = j + 1;
	if (j == 42) {
		break;
	}
} while ((i + j) <= 100);

k = [1, true, false, 0, "BOO!"];

k[2] = true;

m = len(k);

n = foo(100000);