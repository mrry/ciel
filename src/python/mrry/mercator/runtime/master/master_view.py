'''
Created on 8 Feb 2010

@author: dgm36
'''
from cherrypy import HTTPError
from mrry.mercator.runtime.master.task_pool import TASK_ASSIGNED
import simplejson
import cherrypy
from mrry.mercator.runtime.worker.worker_view import DataRoot

class MasterRoot:
    
    def __init__(self, task_pool, worker_pool, block_store, global_name_directory):
        self.worker = WorkersRoot(worker_pool)
        self.task = MasterTaskRoot(global_name_directory, task_pool)
        self.data = DataRoot(block_store)
        self.global_data = GlobalDataRoot(global_name_directory, task_pool, worker_pool)
        #self.cluster = ClusterDetailsRoot()

    @cherrypy.expose
    def index(self):
        return "Hello from the master!"

class WorkersRoot:
    
    def __init__(self, worker_pool):
        self.worker_pool = worker_pool
    
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            worker_descriptor = simplejson.loads(cherrypy.request.body.read())
            worker_id = self.worker_pool.create_worker(worker_descriptor)
            return simplejson.dumps(worker_id)
        elif cherrypy.request.method == 'GET':
            workers = self.worker_pool.get_all_workers()
            return simplejson.dumps(workers)
        else:
            raise HTTPError(405)
        
    @cherrypy.expose
    def random(self):
        return simplejson.dumps('http://%s/' % (self.worker_pool.get_random_worker().netloc, ))
        
    @cherrypy.expose
    def default(self, worker_id, action=None):
        if cherrypy.request.method == 'POST':
            if action == 'ping':
                ping_contents = simplejson.loads(cherrypy.request.body.read())
                worker_status = ping_contents['status']
                news_list = ping_contents['news']
                cherrypy.engine.publish('worker_ping', int(worker_id), worker_status, news_list)
            elif action == 'stopping':
                cherrypy.engine.publish('worker_failed', int(worker_id))
            else:
                raise HTTPError(404)
        else:
            if action is None:
                return simplejson.dumps(self.worker_pool.get_worker_by_id(int(worker_id)).as_descriptor())

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
       
    @cherrypy.expose 
    def default(self, id=None, action=None):
        if id is not None:
            
            task_id = int(id)
            
            # Spawn and commit actions should probably happen on a buffer object for transactional
            # semantics.
            if action == 'spawn':
                if cherrypy.request.method == 'POST':
                    
                    parent_task = self.task_pool.get_task_by_id(task_id)
#                    if not parent_task.state == TASK_ASSIGNED
                    
                    task_descriptors = simplejson.loads(cherrypy.request.body.read())
                    
                    print task_descriptors
                    
                    spawn_result_ids = []
                    
                    # TODO: stage this in a task-local transaction buffer.
                    for task_d in task_descriptors:
                        try:
                            expected_outputs = task_d['expected_outputs']
                        except KeyError:
                            try:
                                num_outputs = task_d['num_outputs']
                                expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                            except:
                                raise
                            
                            task_d['expected_outputs'] = expected_outputs
                        
                        task = self.task_pool.add_task(task_d, task_id)
                        parent_task.children.append(task.task_id)
                        
                        for global_id in expected_outputs:
                            self.global_name_directory.set_task_for_id(global_id, task.task_id)
                        
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
                        self.global_name_directory.add_urls_for_id(int(global_id), urls)
                    
                    self.task_pool.task_completed(task_id)
                    
                    # TODO: Check task for consistency.
                    # TODO: Commit all task activities.

                    return simplejson.dumps(True)
                    
                else:
                    raise HTTPError(405)
            elif action == 'failed':
                if cherrypy.request.method == 'POST':
                    cherrypy.engine.publish('task_failed', task_id, 'RUNTIME_EXCEPTION')
                    return simplejson.dumps(True)
                else:
                    raise HTTPError(405)
            elif action == 'abort':
                self.task_pool.abort(task_id)
            elif action is None:
                if cherrypy.request.method == 'GET':
                    return simplejson.dumps(self.task_pool.tasks[task_id].as_descriptor())
        elif cherrypy.request.method == 'POST':
            # New task spawning in here.
            task_descriptor = simplejson.loads(cherrypy.request.body.read())
            if task_descriptor is not None:
                try:
                    expected_outputs = task_descriptor['expected_outputs']
                except KeyError:
                    try:
                        num_outputs = task_descriptor['num_outputs']
                        expected_outputs = map(lambda x: self.global_name_directory.create_global_id(), range(0, num_outputs))
                    except:
                        expected_outputs = [self.global_name_directory.create_global_id()]
                    task_descriptor['expected_outputs'] = expected_outputs
                
                self.task_pool.add_task(task_descriptor)
                return simplejson.dumps(expected_outputs)
                        
        else:
            if cherrypy.request.method == 'GET':
                return simplejson.dumps(map(lambda x: x.as_descriptor(), self.task_pool.tasks.values()))
                
class GlobalDataRoot:
    
    def __init__(self, global_name_directory, task_pool, worker_pool):
        self.global_name_directory = global_name_directory
        self.task_pool = task_pool
        self.worker_pool = worker_pool
                
    @cherrypy.expose
    def index(self):
        if cherrypy.request.method == 'POST':
            # Create a new global ID, and add the POSTed URLs if any.
            urls = simplejson.loads(cherrypy.request.body.read())
            id = self.global_name_directory.create_global_id(urls)
            return simplejson.dumps(id)
        
    @cherrypy.expose
    def default(self, id, attribute=None):
        if attribute is None:
            if cherrypy.request.method == 'POST':
                # Add a new URL for the global ID.
                urls = simplejson.loads(cherrypy.request.body.read())
                assert urls is list
                try:
                    self.global_name_directory.add_urls_for_id(int(id), urls)
                    return
                except KeyError:
                    raise HTTPError(404)
            elif cherrypy.request.method == 'GET':
                # Return all URLs for the global ID.
                try:
                    urls = self.global_name_directory.get_urls_for_id(int(id))
                    if len(urls) == 0:
                        cherrypy.response.status = 204
                    return simplejson.dumps(urls)
                except KeyError:
                    raise HTTPError(404)
            raise HTTPError(405)
        elif attribute == 'task':
            if cherrypy.request.method == 'GET':
                task_id = self.global_name_directory.get_task_for_id()
                task = self.task_pool.get_task_by_id(task_id)
                task_descriptor = task.as_descriptor()
                task_descriptor['is_running'] = task.worker_id is not None
                if task.worker_id is not None:
                    task_descriptor['worker'] = self.worker_pool.get_worker_by_id(task.worker_id).as_descriptor()
                return simplejson.dumps(task_descriptor)
            else:
                raise HTTPError(405)
        else:
            raise HTTPError(404)
