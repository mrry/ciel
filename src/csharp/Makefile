all:
	cd interface; make; cd ..
	cd loader; make; cd ..
	cd tests; make; cd ..
	mkdir -p bin
	cp loader/loader.exe bin
	cp interface/task.dll bin