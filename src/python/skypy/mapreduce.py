
def shuffle(inputs, num_outputs):
    return [[inputs[j][i] for j in len(inputs)] for i in range(num_outputs)]

def mapreduce(inputs, mapper, reducer, r):
    map_outputs = map(mapper, inputs)
    reduce_inputs = shuffle(map_outputs, r)
    return map(reducer, reduce_inputs)
