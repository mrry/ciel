
from pprint import PrettyPrinter
from shared.references import SWRealReference

class RefPrettyPrinter(PrettyPrinter):

    def format(self, obj, context, maxlevels, level):
        if isinstance(obj, SWRealReference):
            return (str(obj), False, False)
        else:
            return PrettyPrinter.format(self, obj, context, maxlevels, level)

def sw_pprint(obj, **args):

    pprinter = RefPrettyPrinter(**args)
    pprinter.pprint(obj)

