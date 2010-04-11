aref = ref("file:///tmp/atempfile");

anotheref = ref("file:///tmp/atempfile2");

if ((*aref)[0]) {
   x = 100;
} else {
   x = 555;
}

return x;
