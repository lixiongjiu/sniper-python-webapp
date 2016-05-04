#coding=utf8

__author__='lixiongjiu'

'''
Database operation module.This module is independent with web module.
自定义了对象关系映射
'''

import time,logging
import db


class Field(object):
    '''
    UnitTest
    name=Field(name='name',default=lambda:'bob',primary_key=True,ddl='varchar(50)')
    print name.default
    print name
    '''

    #Field类的私有属性，用于统计该域在table中的位置
    _count=0

    def __init__(self,**kw):
        self.name=kw.get('name',None)
        self._default=kw.get('default',None)
        self.primary_key=kw.get('primary_key',False)
        self.nullable=kw.get('nullable',False)
        self.updatable=kw.get('updatable',True)
        self.insertable=kw.get('insertable',True)
        self.ddl=kw.get('ddl','')
        self._order=Field._count
        Field._count+=1


    #field默认值的get函数，如果_default是一个函数，执行函数返回值
    #否则，直接返回_default的值
    @property
    def default(self):
        d=self._default
        return d() if callable(d) else d

    def __str__(self):
        s=['<%s:%s,%s,default(%s),' % (self.__class__.__name__,self.name,self.ddl,'null' if self._default is None else self._default)]
        self.nullable and s.append('N')
        self.updatable and s.append('U')
        self.insertable and s.append('I')
        s.append('>')
        return ''.join(s)


class StringField(Field):
    '''
    name=StringField(name='passwd')
    print name.default
    print name
    '''

    def __init__(self,**kw):
        #如果没有设置默认值，则设为''
        if not 'default' in kw:
            kw['default']=''
        #如果ddl没有设置，则默认为'varchar(255)'
        if not 'ddl' in kw:
            kw['ddl']='varchar(255)'

        super(StringField,self).__init__(**kw)

class IntegerField(Field):
    '''
    name=IntegerField(name='passwd')
    print name.default
    print name
    '''

    def __init__(self,**kw):

        if not 'default' in kw:
            kw['default']=0
        if not 'ddl' in kw:
            kw['ddl']='bigint'

        super(IntegerField,self).__init__(**kw)


class FloatField(Field):
    '''
    name=FloatField(name='passwd')
    print name.default
    print name
    '''
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default']=0.0
        if not 'ddl' in kw:
            kw['ddl']='real'

        super(FloatField,self).__init__(**kw)


class BooleanField(Field):
    '''
    boolean=BooleanField(name='Bool')
    print boolean.default
    print boolean
    '''

    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default']=False
        if not 'ddl' in kw:
            kw['ddl']='bool'

        super(BooleanField,self).__init__(**kw)


class TextField(Field):
    '''
    TextField域，用于存储文本数据
    '''
    def __init__(self,**kw):
        if not 'default' in kw:
            kw['default']=''
        if not 'ddl' in kw:
            kw['ddl']='text'

        super(TextField,self).__init__(**kw)

class BlobField(Field):
    '''
    BlobField,用于存储二进制数据
    '''
    def  __init__(self,**kw):
        if not 'default' in kw:
            kw['default']=''
        if not 'ddl' in kw:
            kw['ddl']='blob'

        super(BlobField,self).__init__(**kw)


class VersionField(Field):
    def __init__(self,name=None):
        super(VersionField, self).__init__(name=name,default=0,ddl='bigint')


def _gen_sql(table_name,mappings):
    '''
    这是一个内部函数，用于生成建表

    nameF=StringField(name='name',primary_key=True)
    ageF=IntegerField(name='age')
    d=dict(name=nameF,age=ageF)
    print _gen_sql('user',d)
    '''

    pk=None
    sql=['-- generating SQL for %s:' % table_name,'create table %s (' % table_name]
    #遍历每一个域对象（mapping是 域名->域对象的结构）
    for f in sorted(mappings.values(),lambda x,y:cmp(x._order,y._order)):
        #每一个域对象若没有ddl，都报错
        if not hasattr(f,'ddl'):
            raise StandardError('no dll in field "%s".' % f.name)

        ddl=f.ddl
        nullable=f.nullable
        default=' default `%s`'% f.default
        if f.primary_key:
            pk=f.name

        sql.append((nullable and ' %s %s' % (f.name,ddl) or ' %s %s not null' % (f.name,ddl))+default)

    sql.append(' primary key(%s)' % pk)
    sql.append(');')
    return '\n'.join(sql)

_triggers=frozenset(['pre_insert','pre_update','pre_delete'])


class ModelMetaclass(type):
    '''
    Metaclass for model,convert attributes to maping
    '''

    def __new__(cls,name,bases,attrs):
        #skip base Model class
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)

        #store all subclass info:
        # Model子类没有子类
        if not hasattr(cls,'subclasses'):
            cls.subclasses={}
        if not name in cls.subclasses:
            cls.subclasses[name]=name
        else:
            logging.warning('Redefing class:%s' % name)


        logging.info('Scan ORMapping %s...' % name)
        mappings=dict()
        primary_key=None
        for k,v in attrs.iteritems():
            #将所有属性名到Field的映射装换成mappings
            if isinstance(v,Field):
                if not v.name:
                    v.name=k
                logging.info('Found mapping:%s=>%s' % (k,v))
                #check duplicate primary key:
                if v.primary_key:
                    if primary_key:
                        raise TypeError('Cannot define more than 1 primary key in class:%s' % name)
                    if v.updatable:
                        logging.warning('NOTE:change primary key to non-updatable.')
                        v.updatable=False

                    if v.nullable:
                        logging.warning('NOTE:change primary key to non-nullable.')
                        v.nullable=False
                    primary_key=v
                mappings[k]=v

        if not primary_key:
            raise TypeError('Primary key not defined in class:%s' % name)
        for k in mappings.iterkeys():
            attrs.pop(k)
        #没有指定表名
        if not '__table__' in attrs:
            attrs['__table__']=name.lower()
        attrs['__mappings__']=mappings
        attrs['__primary_key__']=primary_key
        attrs['__sql__']=_gen_sql(attrs['__table__'],mappings)
        for trigger in _triggers:
            if not trigger in attrs:
                attrs[trigger]=None
        return type.__new__(cls,name,bases,attrs)


class Model(dict):
    '''
    Base class for ORM.
    '''
    __metaclass__=ModelMetaclass

    def __init__(self,**kw):
        super(Model,self).__init__(**kw)

    def __getattr__(self,key):
        try:
            return self[key]
        except:
            raise AttributeError("'Dict' object has not attribute '%s'" % key)

    def __setattr__(self,key,value):
        self[key]=value

    #类方法，因为这是类（表）本身的属性，例如主键，查询
    @classmethod
    def get(cls,pk):
        '''
        Get by primary key
        '''
        d=db.select_one('select * from %s where %s=?' % (cls.__table__,cls.__primary_key__.name),pk)
        #返回本类对象
        return cls(**d) if d else None

    @classmethod
    def find_first(cls,where,*args):
        '''
        Find by where clause and return one result. If multiple results found,
        only the first one returned. If no result found, return None.
        '''
        d=db.select_one('select * from %s %s' % (cls.__table__,where),*args)
        return cls(**d) if d else None

    @classmethod
    def find_all(cls,*args):
        '''
        Find all and return list
        '''
        L=db.select('select * from %s' % cls.__table__)
        # 返回对象list
        return [cls(**d) for d in L]

    @classmethod
    def find_by(cls,where,*args):
        '''
        Find by where clause and return list.
        '''
        L=db.select('select * from %s %s' % (cls.__table__,where),*args)
        return [cls(**d) for d in L]

    @classmethod
    def count_all(cls):
        '''
        Find by 'select count(pk) from table' and return integer.
        '''
        return db.select_int('select count(%s) from %s' % (cls.__primary_key__.name,cls.__table__))

    @classmethod
    def count_by(cls,where,*args):
        '''
        Find by 'select count(pk) from table where ..' and return integer.
        '''
        return db.select_int('select count(%s) from %s %s' % (cls.__primary_key__.name,cls.__table__,where),*args)

    def update(self):
        self.pre_update and self.pre_update()
        L=[]
        args=[]
        #找出所有可更新的field
        for k,v in self.__mappings__.iteritems():
            if v.updatable:
                if hasattr(self,k):
                    arg=getattr(self,k)
                else:
                    arg=v.default
                    setattr(self,k,arg)
                L.append('%s=?' % k)
                args.append(arg)
        pk=self.__primary_key__.name
        args.append(getattr(self,pk))
        db.update('update %s set %s where %s=?' % (self.__table__,','.join(L),pk),*args)
        return self

    def delete(self):
        self.pre_delete and self.pre_delete()
        pk=self.__primary_key__.name
        if not hasattr(self,pk):
            f=self.__mappings__[pk]
            setattr(self,pk,f.default)
        args=(getattr(self,pk),)
        db.delete('delete from %s where %s=?' % (self.__table__,pk),*args)
        return self

    def insert(self):
        self.pre_insert and self.pre_insert()
        params={}
        for k,v in self.__mappings__.iteritems():
            if v.insertable:
                if not hasattr(self,k):
                    setattr(self,k,v.default)
                params[v.name]=getattr(self,k)
        db.insert(self.__table__,**params)
        return self

if __name__ == '__main__':
    class User(Model):
        name=StringField(name='name')
        age=IntegerField(name='age',default=20)
        id=StringField(name='id',primary_key=True,default=db.next_id)

    # print '\nsql:'+User.__sql__
    # print 'pk:'+User.__primary_key__.name
    # print 'table_name:'+User.__table__

    user=User(name='Bob',age='20',id=1)

    db.create_engine('root','123456','python_webapp')
    # user.insert()
    user.age=22
    user.update()
    print User.count_all()
    print User.count_by('where age>?',2)
    # print User.find_all()
    print User.find_first('where age>?',20)
    # user.delete()


