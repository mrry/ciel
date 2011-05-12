OCAMLMAKEFILE=OCamlMakefile
RESULT=ocamltest

SOURCES=base64.mli base64.ml lwt_fiber.mli lwt_fiber.ml cref.mli cref.ml cmd.ml cwt.ml main.ml test.ml

ANNOTATE=yes
PREDS=camlp4o

PACKS += unix lwt.unix
PACKS += lwt.syntax
PACKS += yojson

LIBS += delimcc

all: nc
	@ :

-include $(OCAMLMAKEFILE)
