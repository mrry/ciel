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

testy = function (x) { return 100 + x; } (50);

lesty = (lambda x, y : 100 + testy + y)(50, 3);

hfunc = lambda x : (lambda y : x + y);

h = hfunc(10);

z = [h(20), h(30), h(40)];

dicty = { "foo" : "bar", "b\n\nad" : h(100) };