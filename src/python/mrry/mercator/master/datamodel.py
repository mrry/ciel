'''
Created on Feb 26, 2010

@author: derek
'''

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text, Table, create_engine
from sqlalchemy.orm.session import sessionmaker
from sqlalchemy import PickleType
import simplejson
from sqlalchemy.orm import relation, scoped_session
from sqlalchemy.schema import ForeignKey

Base = declarative_base()

WORKER_STATUS_IDLE = 0
WORKER_STATUS_BUSY = 1
WORKER_STATUS_FAILED = 2

class Worker(Base):
    
    __tablename__ = 'worker'
    
    id = Column(Integer, primary_key=True)
    uri = Column(String, unique=True)
    status = Column(Integer)

task_input = Table('task_input', Base.metadata,
                   Column('task_id', Integer, ForeignKey('task.id')),
                   Column('datum_id', Integer, ForeignKey('datum.id')))

task_output = Table('task_output', Base.metadata,
                    Column('task_id', Integer, ForeignKey('task.id')),
                    Column('datum_id', Integer, ForeignKey('datum.id')))

class DataRepresentation(Base):
    
    __tablename__ = 'data_representation'
    
    id = Column(Integer, primary_key=True)
    datum_id = Column(Integer, ForeignKey('datum.id'))
    discriminator = Column(String(10))
    
    __mapper_args__ = {'polymorphic_on' : discriminator}
    
    def as_dict(self):
        raise
        
class Datum(Base):
    
    __tablename__ = 'datum'
    
    id = Column(Integer, primary_key=True)
    size = Column(Integer)
    
    representations = relation(DataRepresentation, backref='datum')

TASK_STATUS_BLOCKED = 0
TASK_STATUS_RUNNABLE = 1
TASK_STATUS_RUNNING = 2
TASK_STATUS_COMPLETED = 3

class Task(Base):
    
    __tablename__ = 'task'
    
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    args = Column(PickleType)
    status = Column(Integer)

    inputs = relation(Datum, secondary=task_input)
    outputs = relation(Datum, secondary=task_output)

TASK_ATTEMPT_STATUS_RUNNING = 0
TASK_ATTEMPT_STATUS_COMPLETED = 1
TASK_ATTEMPT_STATUS_FAILED = 2

class TaskAttempt(Base):
    
    __tablename__ = 'task_attempt'
    
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('task.id'))
    worker_id = Column(Integer, ForeignKey('worker.id'))
    status = Column(Integer)
    
    task = relation(Task, backref='attempts')
    worker = relation(Worker, backref='task_attempts')
            
class Workflow(Base):
    
    __tablename__ = 'workflow'
    
    id = Column(Integer, primary_key=True)
    script_text = Column(Text)
    script_context = Column(Text)
    
    tasks = relation(Task, order_by=Task.id, backref='workflow')
    
    def __init__(self, script_text, script_context):
        self.script_text = script_text
        self.script_context = script_context
        self.parsed_ast = None
        self.parsed_context = None
        
    def context(self):
        if self.parsed_context is None:
            self.parsed_context = simplejson.loads(self.script_context)
        return self.parsed_context
        
    def ast(self, parser):
        if self.parsed_ast is None:
            self.parsed_ast = parser.parse(self.script_text)
        return self.parsed_ast

class LocalFile(DataRepresentation):
    
    __mapper_args__ = {'polymorphic_identity' : 'local_file'}
    
    worker_id = Column(Integer, ForeignKey('worker.id'))
    worker = relation(Worker, backref='local_files')
    filename = Column(String)
    
    def as_dict(self):
        return {'type':'file', 'worker':self.worker.hostname, 'filename':self.filename}
    
class WebFile(DataRepresentation):
    
    __mapper_args__ = {'polymorphic_identity' : 'web_file'}
    
    url = Column(String)
    
    def as_dict(self):
        return {'type':'http', 'url':self.url}
    
class Regenerate(DataRepresentation):
    
    __mapper_args__ = {'polymorphic_identity' : 'regenerate'}
    
    task_id = Column(Integer, ForeignKey('task.id'))
    task = relation(Task)
    
    def as_dict(self):
        return {'type':'regenerate', 'task':'task_id'}

engine = create_engine('sqlite:////tmp/mercator.db', echo=True, strategy='threadlocal')
Base.metadata.create_all(engine)

Session = scoped_session(sessionmaker(bind=engine))
    
if __name__ == '__main__':
    

    s = Session()
    
    w = Worker(uri='http://www.mrry.co.uk/')
    s.add(w)

    wf = Workflow("foo", "bar")
    s.add(wf)
    
    d1 = Datum(size=1048576)
    d2 = Datum(size=2097192)
    s.add(d1)
    s.add(d2)
    
    t = Task(inputs=[d1, d2])
    s.add(t)

    wf.tasks = [t]

    s.commit()

    s.close()
    

    