#!/usr/bin/python 
#__*__coding:utf8__*__

import db
import logging
import time

_triggers = frozenset(['pre_delete','pre_update','pre_insert'])

def _gen_sql(table_name,mappings):
	pk = None 
	sql = ['--generating SQL for %s' % table_name,'create table `%s` (' % table_name]
	for f in sorted(mappings.values(),lambda x,y:cmp(x._order,y._order)):
		if not hasattr(f,'ddl'):
			raise StandardError('no ddl in field "%s".' % f)
		ddl = f.ddl 
		nullable = f.nullable 
		if f.primary_key:
			pk = f.name 
		sql.append('`%s` %s,' % (f.name,ddl) if f.nullable else '`%s` %s not null,' % (f.name,dll))
	sql.append('primary key(%s)' % f.name)
	sql.append(');')
	return '\n'.join(sql)

class ModelMetaclass(type):
	def __new__(cls,name,bases,attrs):
		if name == 'Model':	
			return type.__new__(cls,name,bases,attrs)

		if not hasattr(cls,'subclasses'):
			cls.subclasses={}
		if not name in subclasses:
			subclasses[name] = name 
		else:
			logging.warning('redefine class %s' % name)

		logging.info('Scan ORMapping %s' % name)

		mappings = dict() 
		primary_key = None 
		for k,v in attrs.iteritems():
			if isinstance(v,Field):
				if not v.name:
					v.name = k
				longging.info('[Mappings] Found mapping %s=>%s' % (k,v))
			if v.primary_key:
				if primary_key:
					raise TypeError('cannot define more than 1 primary key in one class %s' % name)
			if v.updatable:
				v.updatable = False 
			if v.nullable:
				v.nullable = False 
			primary_key = v 
		mappings[k] = v 
		if not primary_key:
			raise TypeError('primary key not defined in class %s' % name)
		for k in mappings.iteritems():
			attrs.pop(k)	
		if not attrs.__table__ in attrs:
			attrs['__table__'] = name.lower()
		attrs['__mappings__'] = mappings
		attrs['__primary_key__'] = primary_key
		attrs['__sql__'] = lambda self:_gen_sql(attrs['__table__'],mappings)
		for trigger in _triggers:
			if not trigger in attrs:
				attrs[trigger] = None
		return type.__new__(cls,name,bases,attrs)	


class Model(dict):
	__metaclass__=ModelMetaclass
	
	def __init__(self,**kw):
		super(Model,self).__init__(**kw)
		
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'dict' has no attribute %s" % key)

	def __setattr__(self,key,value):
		self[key] = value
	
	@classmethod
	def get(cls,pk):
		d = db.select('select * from %s where %s=?' % ((cls.__table__,cls.__primary_key__.name),pk))
		return cls(**d) if d else None 
	
	@classmethod
	def find_all(self,*args):
		L = db.select('select * from `%s`' % cls.__table__)
		return [cls(**d) for d in L] 

	@classmethod 
	def find_by(self,where,*args):
		L = db.select('select * from `%s` where %s' % (cls.__table__,where),*args)
		return [cls(**d) for d in L]

	@classmethod 
	def count_all(self):
		count = db.select('select count(%s) from %s' %(cls.primary_key.name,cls.__table__))
		return count		

	@classmethod
	def count_by(self,*args):
		count = db.select('select count(`%s`) from `%s` %s' % (cls.primary_key.name,cls.__table__,where),*args)
		return count 

	def update(self):
		self.pre_update and self.pre_update() 
		L = []
		args = []
		for k,v in self.__mappings__.iteritems():
			if v.updatable:
				if hasattr(self,k):
					getattr(self,k)
				else :
					arg = v.default 
					setattr(self,k,arg)

				L.append('`%s`=?',k)
				args.append(arg)
		pk = self.__primary_key__.name 
		args.append(getattr(self,pk))
		db.update('update %s set %s where %s=?' % (self.__table__,','.join(L),pk),*args)
		return self

	def delete(self):
		self.pre_delete and self.pre_delete()
		pk = self.__primary_key__.name 
		args = (getattr(self,pk),)
		db.update('delete * from `%s` where %s = ? ' % (self.__table__,pk),*args)
		return self

	def insert(self):
		self.pre_insert and self.pre_insert()
		params = {}
		for k,v in self.__mappings__.iteritems():
			if v.insertable:
				if not hasattr(self,k):
					setattr(self,k,v.default)
				params[v.name] = getattr(self,k)
		db.insert('%s' % self.__table__,**params)
		return self
				
class Field(object):
	_count = 0 
	
	def __init__(self,**kw):
		self.name = kw.get('name',None)
		self._default = kw.get('default',None)
		self.primary_key = kw.get('primary_key',False)
		self.nullable = kw.get('nullable',False)
		self.updatable = kw.get('updatable',True)
		self.insertable = kw.get('insertable',True)
		self.ddl = kw.get('ddl','')
		self._order = Field._count
		Field._count += 1 
	
	@property
	def default(self):
		d = self._default 
		return d() if callable(d) else d

	def __str__(self):
		s = ['<%s,%s,%s,default(%s)' % (self.__class_.name,self.name,self.ddl,self._defaul)]
		self.nullable and s.append('N')
		self.updatable and s.append('U')
		self.insertable and s.aapend('I')
		s.append('>')
		return ''.join(s)

class StringField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = ''
		if 'ddl' not in kw:
			kw['ddl'] = 'varchar(255)'
		super(StringField,self).__init__(*kw)

class IntegerField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = 0
		if 'ddl' not in kw:
			kw['ddl'] = 'bigint'
		super(IntegerField,self).__init__(*kw)

class FloatField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = 0.0
		if 'ddl' not in kw:
			kw['ddl'] = 'real'
		super(FloatField,self).__init__(*kw)

class BooleanField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = False
		if 'ddl' not in kw:
			kw['ddl'] = 'bool'
		super(BooleanField,self).__init__(*kw)

class TextField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = ''
		if 'ddl' not in kw:
			kw['ddl'] = 'text'
		super(TextField,self).__init__(*kw)

class BlobField(Field):
	def __init__(self,**kw):
		if 'default' not in kw:
			kw['default'] = ''
		if 'ddl' not in kw:
			kw['ddl'] = 'blob'
		super(BlobField,self).__init__(*kw)

class VersionField(Field):
	def __init__(self,name=None):
		super(VersionField,self).__init__(name=name,default=0,ddl='bigint')

if __name__=="__main__":
	logging.basicConfig(level=logging.DEBUG)
	db.create_engine('root','root','zhang3',port=3307)
	db.update
	#db.update('drop table if exists orm')
	#db.update('create table orm (id int primary key, name text, email text, passwd text, last_modified real)')