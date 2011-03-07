
import skypy

def skypy_main():

    print >>sys.stderr, "SkyPy example producer:", len(skypy.other_output), "outputs"

    for i, id in skypy.anonymous_outputs:
        with skypy.open_output(id) as file_out:
            file_out.fp.write("Skypy writing output %d" % i)

    return "Wrote %d outputs" % len(skypy.anonymous_outputs)

