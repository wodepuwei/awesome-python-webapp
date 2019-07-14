#!/usr/bin/python 
#__*__coding:utf8__*__

import logging 
import threading 
import logging 
import functools

engine = None

def create_engine(user,passwd,database,host='127.0.0.1',port=3306,**kw):
	import mysql.connector
	global engine 
	if engine is not None:
		raise DBError('Engine is already initialized')
	params=dict(user=user,passwd=passwd,database=database,host=host,port=port)
	defaults=dict(use_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
	for k,v in defaults.iteritems():
		params[k] = kw.pop(k,v)
	params.update(kw)
	params['buffered'] = True 
	engine = _Engine(lambda:mysql.connector.connect(**params)) 
	logging.info('Init mysql engine %s ' % hex(id(engine)))	 

class _Engine(object):
	def __init__(self,connect):
		self._connect = connect
	def connect(self):
		return self._connect 

class DBError(object):
	pass 

class _DbCtx(threading.local):
	def __init__(self):
		self.connection = None
		self.transactions = 0 

	def is_init(self):
		return not self.connection is None 

	def init(self):
		self.connection = _LasyConnection()
		self.transctions = 0 
	def cleanup(self):
		self.connection.cleanup()
		self.connection = None 

	def cursor(self):
		return self.connection.cursor() 

_db_ctx = _DbCtx()

class _ConnectionCtx(object):
	def __enter__(self):
		global _db_ctx 
		self.should_cleanup = False 
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_cleanup = True
		return self 

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx 
		if self.should_cleanup:
			_db_ctx.cleanup() 

def connection():
	return _ConnectionCtx()

def with_connection(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _ConnectionCtx():
			func(*args,**kw)
		return _wrapper 

@with_connection
def _select(sql,*args):
	global _db_ctx 
	cursor = None 
	sql = sql.replace('?','%s')	
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,*args)
		if cursor.description:
			names = [x[0] for x in cursor.description]
		return [Dict(names,x) for x in cursor.fetchall()]
	finally:
		if cursor:
			cursor.close() 

def select(sql,*args):
	return _select(sql,*args)

@with_connection 
def _update(sql,*args):
	global _db_ctx 
	cursor = None 
	sql = sql.replace('?','%s')
	
	try:
		cursor = _db_ctx.connection.cursor()
		cursor.execute(sql,*args)		
		r = cursor.rowcount 
		if _db_ctx.transactions == 0 :
			_db_ctx.connection.commit() 
		return r 
	finally:
		if cursor:
			cursor.close()

def update(sql,*args):
	return _update(sql,*args)

class _TransactionCtx(object):
	def __enter__(self):
		global _db_ctx
		self.should_close_conn = False
		if not _db_ctx.is_init():
			_db_ctx.init()
			self.should_close_conn = True 
		_db_ctx.transactions = _db_ctx.transactions +1 
		return self 

	def __exit__(self,exctype,excvalue,traceback):
		global _db_ctx
		_db_ctx.transactions = _db_ctx.transactions -1 
		try:
			if _db_ctx.transactions == 0: 
				if exctype is None:
					self.commit()
				else:
					self.rollback()

		finally:
			if self.should_close_conn:
				_db_ctx.cleanup()

	def commit(self):
		global _db_ctx
		try:
			_db_ctx.connection.commit()
		except:
			_db_ctx.connection.rollback()
			raise 

	def rollback(self):
		global _db_ctx
		_db_ctx.connection.rollback()

def transaction():
	return _TransactionCtx()

def with_transaction(func):
	@functools.wraps(func)
	def _wrapper(*args,**kw):
		with _TransactionCtx():
			func(*args,**kw) 
	return _wrapper


class Dict(dict):
	def __init__(self,names=(),values=(),**kw):
		super(Dict,self).__init__(**kw)
		for k,v in zip(names,values):
			self[k] = v 
	
	def __getattr__(self,key):
		try:
			return get(key)
		except KeyError:
			raise AttributeError(r"'Dict' has no attribute '%s'" % key)
		
	def __setattr__(self,key,value):
		self[key] = value 

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)
	create_engine('root','root','zhang3',port=3307)
	update('drop table if exists user')	
