'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy import HTTPError
import simplejson
import cherrypy
from mrry.mercator.runtime.worker.worker_view import DataRoot

class MasterRoot:
    
    def __init__(self, worker_pool, block_store, global_name_directory):
        self.ping = PingReceiver()
        self.worker = WorkersRoot(worker_pool)
        self.task = MasterTaskRoot()
        self.data = DataRoot(block_store)
        self.global_data = GlobalDataRoot(global_name_directory)
        #self.cluster = ClusterDetailsRoot()

class PingReceiver:
    
    @cherrypy.expose
    def index(self):
        update_tuple = simplejson.loads(cherrypy.request.body.read())
        worker_id = update_tuple[0]
        update_list = update_tuple[1]
        
        for update in update_list:
            cherrypy.engine.publish("ping_received", worker_id, update)

class WorkersRoot:
    
    def __init__(self, worker_pool):
        self.worker_pool = worker_pool
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            worker_descriptor = simplejson.loads(cherrypy.request.body.read())
            self.worker_pool.create_worker(worker_descriptor)
            return
        elif cherrypy.request.method == 'GET':
            workers = self.worker_pool.get_all_workers()
            return simplejson.dumps(workers)
        else:
            raise HTTPError(405)

class MasterTaskRoot:
    
    def __init__(self, global_name_directory, task_pool):
        self.global_name_directory = global_name_directory
        self.task_pool = task_pool
        
    # TODO: decide how to submit tasks to the cluster. Effectively, we want to mirror
    #       the way workers do it. Want to have a one-shot distributed execution on the
    #       master before sending out continuations to the cluster. The master should
    #       have minimal exec functionality, and should short-circuit the RPCs for spawning
    #       and creating global IDs.
    #
    #       The master might also have some special functionality that can only be provided
    #       by invoking the master (such as cluster details), but this could potentially be
    #       provided at the master.
        
    def default(self, id=None, action=None):
        if id is not None:
            # Spawn and commit actions should probably happen on a buffer object for transactional
            # semantics.
            if action == 'spawn':
                if cherrypy.request.method == 'POST':
                    task_descriptors = simplejson.loads(cherrypy.request.body.read())
                    spawn_result_ids = []
                    
                    # TODO: stage this in a task-local transaction buffer.
                    for task in task_descriptors:
                        try:
                            num_outputs = task['num_outputs']
                            expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                        except:
                            expected_outputs = self.global_name_directory.create_global_id()
                        
                        task['expected_outputs'] = expected_outputs
                        self.task_pool.add_task(task)
                        spawn_result_ids.append(expected_outputs) 
                    
                    return simplejson.dumps(spawn_result_ids)
                    
                else:
                    raise HTTPError(405)
                pass
            elif action == 'commit':
                if cherrypy.request.method == 'POST':
                    commit_bindings = simplejson.loads(cherrypy.request.body.read())
                    
                    # Apply commit bindings (if any), i.e. publish results.
                    for global_id, urls in commit_bindings.items():
                        self.data_store.add_urls_for_id(global_id, urls)
                    
                    # TODO: Check task for consistency.
                    # TODO: Commit all task activities.

                    return simplejson.dumps(True)
                    
                else:
                    raise HTTPError(405)
            elif action is None:
                pass
        elif cherrypy.request.method == 'POST':
            # New task spawning in here.
            task_descriptor = simplejson.loads(cherrypy.request.body.read())
            if task_descriptor is not None:
                cherrypy.engine.publish('execute_task', task_descriptor)
                return
        else:
            raise HTTPError(405)
            
class GlobalDataRoot:
    
    def __init__(self, global_name_directory):
        self.global_name_directory = global_name_directory

    def index(self, id):
        if cherrypy.request.method == 'POST':
            # Create a new global ID, and add the POSTed URLs if any.
            urls = simplejson.loads(cherrypy.request.body.read())
            id = self.global_name_directory.create_global_id(urls)
            return simplejson.dumps(id)
        
    def default(self, id):
        if cherrypy.request.method == 'POST':
            # Add a new URL for the global ID.
            urls = simplejson.loads(cherrypy.request.body.read())
            assert urls is list
            try:
                self.global_name_directory.add_urls_for_id(id, urls)
                return
            except KeyError:
                raise HTTPError(404)
        elif cherrypy.request.method == 'GET':
            # Return all URLs for the global ID.
            try:
                urls = self.global_name_directory.get_urls_for_id(id)
                return simplejson.dumps(urls)
            except KeyError:
                raise HTTPError(404)
        raise HTTPError(405)
    
#class ClusterDetailsRoot:
#    
#    def __init__(self):
#        pass
#    
#    # This could provide a way to provide details about the cluster in the 
