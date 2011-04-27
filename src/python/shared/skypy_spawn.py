
class SkyPySpawn:

    def __init__(self, ret_output, extra_outputs):

        self.ret_output = ret_output
        self.extra_outputs = extra_outputs
        self.apparent_list = [ret_output]
        self.apparent_list.extend(extra_outputs)

    def __getitem__(self, i):

        return self.apparent_list[i]

    def __len__(self):

        return len(self.apparent_list)
