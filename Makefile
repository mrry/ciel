PYTHON:=`which python`
DESTDIR=/
BUILDIR=$(CURDIR)/debian/ciel
PROJECT=ciel
VERSION=0.1

all:
	@ :

dist/.cookie:
	rm -rf dist
	mkdir -p dist

dist/ciel-$(VERSION).tar.gz: dist/.cookie
	$(PYTHON) setup.py sdist

.PHONY: source
source: dist/ciel-$(VERSION).tar.gz
	@ :

dist/ciel_%.orig.tar.gz: dist/ciel-%.tar.gz
	cp $< $@

.PHONY: debsource
debsource: dist/ciel_$(VERSION).orig.tar.gz
	@ :

.PHONY: install
install:
	$(PYTHON) setup.py install --root $(DESTDIR) 

.PHONY: rpm
rpm:
	$(PYTHON) setup.py bdist_rpm #--post-install=rpm/postinstall --pre-uninstall=rpm/preuninstall

.PHONY: deb
deb: dist/ciel_$(VERSION).orig.tar.gz
	cd dist && rm -rf ciel-$(VERSION) && \
	  tar -zxvf ciel_$(VERSION).orig.tar.gz && \
	  cp -r ../debian ciel-$(VERSION) && \
	  cd ciel-$(VERSION) && debuild -us -uc

.PHONY: clean
clean:
	$(PYTHON) setup.py clean
	rm -rf dist/ build/ src/python/ciel.egg-info/
	find . -name '*.pyc' -delete
