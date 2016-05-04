# coding=utf-8

__author__ = 'lixiongjiu'

'''
Database operation module.
'''
import time
import uuid
import functools
import threading
import logging

# Dict object:


class Dict(dict):
    """
    Simple dict but support access as x.y style
    #Unit test
    d1=Dict(name='bob',age=12)
    print d1.name
    print d1.age
    d2=Dict(('name','age'),('bob',12))
    print d2.name
    print d2.age
    """

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has not attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value


def next_id(t=None):
    '''
    返回50位的string序列作为主键（15位时间参数+32位UUID+000）
    #UnitTest
    print next_id(t=time.time())
    '''
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t * 1000), uuid.uuid4().hex)


def _profile(start, sql=''):
    '''
    #生成数据库操作的日志信息
    #UnitTest
    t=time.time()
    _profile(t,'test')
    '''
    t = time.time() - start
    # 操作时间过长，发出警告
    if t > 0.1:
        logging.warning('[PROFILING][DB] %s:%s' % (t, sql))

    else:
        logging.info('[PROFILING][DB] %s:%s' % (t, sql))


class DBError(Exception):
    pass


class MultiColumnsError(DBError):
    pass


#属于线程内部的变量，所以用_命名
class _LasyConnection(object):
    """
    封装了数据库连接对象的操作（获取游标，提交，回滚，关闭）
    """
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            # 获取数据库连接对象
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection=self.connection
            self.connection=None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()


#属于线程内部的变量，所以用_命名
class _DbCtx(threading.local):
    '''
    单个线程的全局变量，相当于数据库操作中，每一个线程是一个单位，维持一个连接，
    这个单位一般叫做上下文
    '''
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        logging.info('open lazy connection...')
        #这个上下文用封装的内部类位单位进行操作
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

# thread-local db context:定义线程内部全局变量用作上下文
_db_ctx = _DbCtx()

# global engine object:全局变量engine
# 该变量用于维护数据库连接的信息和选项，所有的线程共享这个变量
# 通过create_engine()初始化
engine = None


class _Engine(object):
    '''
    传进函数类型的变量
    connect函数返回函数执行的结果
    _connect是私有变量，类似于指向函数的指针，所有的线程都共享这个变量
    '''
    def __init__(self, connect):
        super(_Engine, self).__init__()
        self._connect = connect

    def connect(self):
        return self._connect()


def create_engine(user, password, database, host='127.0.0.1', port='3306', **kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')

    # 利用dict的构造函数来初始化参数字典
    params = dict(user=user, password=password,
                  database=database, host=host, port=port)
    # 设置默认数据库连接选项
    defaults = dict(use_unicode=True, charset='utf8',
                    collation='utf8_general_ci', autocommit=False)
    # 根据**kw参数来设置选项（没有对应选项时用默认参数代替）
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)

    # 根据参数**kw设置其他选项（user，password等）
    params.update(kw)
    # params['bufferd'] = True

    # 通过lambda将函数类型变量传递给_Engine对象，真正的connection对象
    # 只有在执行_Engine.connect()函数（执行连接操作）后才有
    engine = _Engine(lambda: mysql.connector.connect(**params))

    logging.info("Init mysql engine <%s> ok." % hex(id(engine)))


#以下部分设计十分精妙，数据库的一个操作，例如select，update等
# 都用一个连接来完成。那么每一个操作都要对应打开连接，操作，关闭连接的过程
# 其中，打开连接和关闭连接是可重用的，于是一般人能想到用with实现
# 但是使用with还不够简介，with再封装一下，变成装饰器会使代码更加简介
# connection with的上下文管理器
class _ConnectionCtx(object):
    '''
    该上下文管理器用于connection对象的创建和回收
    每一个操作，例如insert，update
    '''
    def __enter__(self):
        global _db_ctx
        self.should_cleanup=False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup=True
        return self

    def __exit__(self,exctype,excvalue,traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def with_connection(func):
    '''
    Decorator:connection的装饰器，形成@注解的方式来管理connection对象的初始化和回收
    '''

    #采用装饰器重定义func函数（func代表数据库的一个操作，例如select）
    #使得func函数被with装饰，无需func来管理connection对象
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        with _ConnectionCtx():
            return func(*args,**kw)

    return _wrapper

class _TransactionCtx(object):
    '''
    事务管理的上下文管理器
    '''
    def __enter__(self):
        global _db_ctx
        self.should_close_conn=False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn=True
        _db_ctx.transactions+=1
        logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
        return self

    def __exit(self,exctype,excvalue,traceback):
        global _db_ctx
        _db_ctx.transactions-=1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed.try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')


def with_transaction(func):
    '''
    将事务管理的with形式升级为装饰器模式
    '''
    @functools.wraps(func)
    def _wrapper(*args,**kw):
        _start=time.time()
        with _TransactionCtx():
            return func(*args,**kw)

        _profile(_start)

    return _wrapper

def _select(sql,first,*args):
    '''
    执行select sql
    '''
    global _db_ctx
    cursor=None

    sql=sql.replace('?','%s')
    logging.info('SQL:%s, ARGS:%s' % (sql,args))
    try:
        cursor=_db_ctx.connection.cursor()
        cursor.execute(sql,args)
        #返回的字段中，属性描述是以tupe的形式存在的，每个tupe的第一个元素是属性名
        if cursor.description:
            name=[ x[0] for x in cursor.description]

        #select one
        if first:
            values=cursor.fetchone()
            #返回了0个元组
            if not values:
                return None
            return Dict(name,values)

        #select more than one
        return [Dict(name,x) for xx in cursor.fetchall()]

    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql,*args):
    return _select(sql,True,*args)


@with_connection
def select_int(sql,*args):
    d=_select(sql,True,*args)

    if len(d)!=1:
        raise MultiColumnsError('Expect only one column.')

    return d.values()[0]


@with_connection
def select(sql,*args):
    return _select(sql,False,*args)


@with_connection
def _update(sql,*args):
    global _db_ctx
    cursor=None
    sql=sql.replace('?','%s')
    logging.info('SQL:%s, ARGS:%s' % (sql,args))
    try:
        cursor=_db_ctx.connection.cursor()
        cursor.execute(sql,args)
        r=cursor.rowcount

        if _db_ctx.transactions==0:
            logging.info('auto commit')
            _db_ctx.connection.commit()

        return r

    finally:
        if cursor:
            cursor.close()


def insert(table,**kw):
    # 反连接，将key，value组成的元组分开
    cols,args=zip(*kw.iteritems())
    sql='insert into %s (%s) values(%s)' % (
                table,
                ','.join(['%s' % col for col in cols]),
                ','.join(['?' for i in range(len(cols)) ])
                )
    return _update(sql,*args)


def update(sql,*args):
    return _update(sql,*args)

def delete(sql,*args):
    return _update(sql,*args)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    create_engine('root','123456','python_webapp')
    # d1=Dict(a=1,b=1)
    # d2=Dict(a=3,b=2)
    # insert('test',**d1)
    # insert('test',**d2)
    results=select_one('select * from test where a=1')
    print results

