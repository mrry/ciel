'''
Created on Feb 26, 2010

@author: derek
'''

import sqlalchemy.ext.declarative
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Text
import simplejson
from mrry.mercator.cloudscript.parser import CloudScriptParser
from sqlalchemy.orm import relation
from sqlalchemy.schema import ForeignKey

Base = declarative_base()

class Worker(Base):
    
    __tablename__ = 'worker'
    
    id = Column(Integer, primary_key=True)
    uri = Column(String, unique=True)
    
    def __init__(self, uri):
        self.uri = uri
        
class Workflow(Base):
    
    __tablename__ = 'workflow'
    
    id = Column(Integer, primary_key=True)
    script_text = Column(Text)
    script_context = Column(Text)
    
    tasks = relation(Task, order_by=Task.id, backref='workflow')
    
    def __init__(self, text, context):
        self.script_text = text
        self.script_context = script_context
        self.parsed_ast = None
        self.parsed_context = None
        
    def context(self):
        if self.parsed_context is None:
            self.parsed_context = simplejson.loads(self.script_context)
        return self.parsed_context
        
    def ast(self, parser=CloudScriptParser()):
        if self.parsed_ast is None:
            self.parsed_ast = parser.parse(self.script_text)
        return self.parsed_ast
    
class Task(Base):
    
    __tablename__ = 'task'
    
    id = Column(Integer, primary_key=True)
    workflow_id = Column(Integer, ForeignKey('workflow.id'))
    
    inputs = relation(Data, order_by=Data.id, backref='task')
    outputs = relation(Data, order_by=Data.id, backref)
    
    def __init__(self, inputs):
        pass