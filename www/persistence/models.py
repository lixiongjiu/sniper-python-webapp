#coding=utf8

import time,uuid
from db import next_id
from orm import Model,StringField,BooleanField,FloatField,TextField,_gen_sql

# 用户信息
class User(Model):
    __table__='users'

    id=StringField(primary_key=True,default=next_id,ddl='varchar(50)')
    email=StringField(updatable=False,ddl='varchar(50)')
    password=StringField(ddl='varchar(50)')
    admin=BooleanField()
    name=StringField(ddl='varchar(50)')
    image=StringField(ddl='varchar(500)')
    created_at=FloatField(updatable=False,default=time.time)

# print _gen_sql(User.__table__,User.__mappings__)

# 博客
class Blog(Model):
    __table__='blogs'

    id=StringField(primary_key=True,default=next_id,ddl='varchar(50)')
    user_id=StringField(updatable=False,ddl='varchar(50)')
    user_image=StringField(ddl='varchar(500)')
    name=StringField(ddl='varchar(50)')
    summary=StringField(ddl='varchar(200)')
    content=TextField()
    created_at=FloatField(updatable=False,default=time.time)

# 评论
class Comment(Model):
    __table__='comments'

    id=StringField(primary_key=True,default=next_id,ddl='varchar(50)')
    blog_id=StringField(updatable=False,ddl='varchar(50)')
    user_id=StringField(updatable=False,ddl='varchar(50)')
    user_name=StringField(ddl='varchar(50)')
    user_image=StringField(ddl='varchar(500)')
    content=TextField()
    created_at=FloatField(updatable=False,default=time.time)
