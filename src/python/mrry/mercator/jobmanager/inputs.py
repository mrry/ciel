'''
Created on 17 Feb 2010

@author: dgm36
'''
import socket
import urllib
import os

class JobOutputs:
    
    def __init__(self, job, outputs):
        self.job = job
        self.outputs = []
        self.output_map = {}        
        self.process_list(self.outputs, outputs)
        
    def process_list(self, processed_output_list, unprocessed_output_list):
        for output in unprocessed_output_list:
            if type(output) is list:
                new_sublist = []
                processed_output_list.append(new_sublist)
                self.process_list(new_sublist, output)
            else:
                processed_output_list.append(self.create_concrete_output(output))
                
    def create_concrete_output(self, output):
        jo = JobOutput(output["id"], self.job)
        self.output_map[jo.id] = jo
        return jo

    def to_external_format(self, output):
        if type(output) is list:
            return { "output_type": "list", "value": map(self.to_external_format, output) }
        else:
            return { "output_type": "file", "value": output.filename }

    def get_output_structure(self):
        return self.to_external_format(self.outputs)
    
class JobOutput:
    
    def __init__(self, id, job):
        self.id = id
        self.filename = "%s-output-%s" % (job.id, id)
        

class JobArguments:
    
    def __init__(self, job, inputs):
        self.job = job
        self.inputs = []
        self.pending_inputs = set()
        self.process_list(self.inputs, inputs)


    def process_list(self, processed_input_list, unprocessed_input_list):
        for input in unprocessed_input_list:
            if type(input) is list:
                new_sublist = []
                processed_input_list.append(new_sublist)
                self.process_list(new_sublist, input)
            else:
                # We have a real input.
                processed_input_list.append(self.create_concrete_input(input))
                
    def create_concrete_input(self, input):
        if input["input_type"] == "literal":
            return self.create_literal_input(input)
        elif input["input_type"] == "compound":
            return self.create_compound_input(input)
        elif input["input_type"] == "http":
            return self.create_http_input(input)
        elif input["input_type"] == "localfile":
            return self.create_localfile_input(input)
        else:
            raise KeyError(input["input_type"])
        
    def create_literal_input(self, input):
        return JobLiteralInput(self.job, input["value"])
    
    def create_compound_input(self, input):
        self.pending_inputs.add(input["id"])
        print input["sources"]
        sources = map(self.create_concrete_input, input["sources"])
        return MultipleSourceJobInput(self.job, input["id"], sources)
    
    def create_http_input(self, input):
        self.pending_inputs.add(input["id"])
        return HttpJobInput(self.job, input["id"], input["url"])

    def create_localfile_input(self, input):
        self.pending_inputs.add(input["id"])
        return LocalFileJobInput(self.job, input["id"], input["host"], input["filename"])

    def to_external_format(self, input):
        if type(input) is list:
            return { "input_type": "list", "value": map(self.to_external_format, input) }
        elif input.is_literal():
            return { "input_type": "literal", "value": input.value }
        else:
            return { "input_type": "file", "filename": input.filename }

    def get_input_structure(self):
        return self.to_external_format(self.inputs)

class JobInput:

    def __init__(self, job):
        self.job = job

class JobLiteralInput(JobInput):
    
    def __init__(self, job, value):
        JobInput.__init__(self, job)
        self.value = value
        
    def is_literal(self):
        return True
    
    def to_external_format(self):
        return { "input_type": "literal", "value": self.value }

class JobDataInput(JobInput):
    
    def __init__(self, job, id, parent=None):
        self.job = job
        self.id = id
        self.filename = None
        self.parent = parent
        
    def fetch(self):
        raise

    def is_literal(self):
        return False

class HttpJobInput(JobDataInput):
    
    def __init__(self, job, id, url):
        JobDataInput.__init__(self, job, id)
        self.url = url
        
    def fetch(self):
        filename, _ = urllib.urlretrieve(self.url)
        return filename

    def to_external_format(self):
        return { "input_type": "http", "id": self.id, "url": self.url }
        
class LocalFileJobInput(JobDataInput):
    
    def __init__(self, job, id, host, filename):
        JobDataInput.__init__(self, job, id)
        self.host = host
        self.filename = filename
        
    def fetch(self):
        if self.host != socket.getfqdn():
            raise
        elif not os.path.isfile(self.filename):
            raise
        else:
            return self.filename
        
    def to_external_format(self):
        return { "input_type": "localfile", "id": self.id, "host": self.host, "filename": self.filename }
        
class MultipleSourceJobInput(JobDataInput):
    
    def __init__(self, job, id, sources):
        JobDataInput.__init__(self, job, id)
        self.sources = sources
        for source in sources:
            source.parent = self
        
    def fetch(self):
        for source in self.sources:
            try:
                filename = source.fetch()
                return filename
            except:
                pass
        raise
    
    def to_external_format(self):
        return { "input_type": "compound", "id": self.id, "sources": [source.to_external_format() for source in self.sources] }
    