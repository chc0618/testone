__author__ = 'mz'
#-*-coding:utf-8 -*-

import MySQLdb
from bs4 import BeautifulSoup
import os,sys
import re, traceback
import time

#修改系统默认的编码
reload(sys)
sys.setdefaultencoding('utf-8')
#########  1记得修改log保存路径  2数据库
#xml路径
#filepath='D:\\qqxml\\lht'
#filepath='F:\\qqdataset\\65092196 1\\1'
#filepath='F:\\qqdataset\\83563561new\\83563561'
##filepath='F:\\qqdataset\\zhangnan85188045\\zhangnan85188045'
##filepath='F:\\qqdataset\\qqsunhao'
#filepath='F:\\qqdataset\\qqzonelht\\gaoge'
filepath='L:\\dataset\\qqdataset\\qqzone_puiples_tiandafuxiao'

#链接数据库并获得当前游标
db=MySQLdb.connect( host = 'localhost' , user = 'root' , passwd = '123456' , db = 'tiandafuxiao' , charset = 'utf8')
cursor =db.cursor()

# 数据库字段
UserID=""
feed_type=""
content=""
PubTime=""
QQ1=""
QQ2=""
Reply_Time=""
Reply_Content=""
cnt = 0
###############

#通过soup对象解析xml文件，将解析后的数据写入数据库
def writeDB(soup):

    #获得Info标签信息
    Info= soup.findAll('info')[0]
    Basic=Info.findAll('basic')[0]
    #全局化UserID
    global UserID

    UserID=Basic['uin']


    #获得MsgFeeds标签
    MsgFeeds = soup.findAll('msgfeeds')[0]
    #获得Blog标签
    Blog = soup.findAll('blog')[0]
    #获得MsgBoard标签
    MsgBoard = soup.findAll('msgboard')[0]
    
    #分别处理相应标签信息
    handleInfo(Basic)
    handleMsg(MsgFeeds)
    #handleBlog(Blog)
    #handleBoard(MsgBoard)


# 处理用户信息
def handleInfo(Basic):
    try:

        #分别获取相应标签的各个属性
        birth = Basic['birthday']

        SpName = Basic['spacename']
        #修正SpName
        SpName = SpName.replace("'","\\'")

        QQ = Basic['uin']

        NickName = Basic['nickname']
        #修正SpNmae
        NickName = NickName.replace("'","\\'")

        Location = Basic['province']
        Career  = Basic['career']
        Age = Basic['age']

        #生成插入记录命令
        sql = "insert into qq_info values( '%s' , '%s' , '%s' , '%s' , '%s' , '%s' , %s )"%( birth, SpName , QQ , NickName , Location , Career , Age)

        #执行插入命令
        cursor.execute(sql)
        db.commit()

        #print 'info finished'
    except:
        print traceback.print_exc()
        pass


# 处理说说信息
def handleMsg(MsgFeeds):

    #标记qq_info的类型
    feed_type="message"

    #获取属性message所有内容
    messages = MsgFeeds.findAll('message')
    content = ''

    #枚举每一条message
    for message in messages:

        #获取message中content属性
        try:
            content=message.findAll('content')[0].text[:200]
        except:
            pass

        #获取message中time属性
        PubTime=message['time']

        #设置qq的默认值 防止没有评论的情形出现
        QQ1 ="00000000"
        QQ2="00000000"

        #获取message中的comments属性
        comments=message.findAll('comments')[0]
        #从刚刚获得comments中获取comment属性
        comments=comments.findAll('comment')

        #debug info
        print UserID + PubTime

        #如果没有评论
        if len(comments)==0:
            sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content,PubTime,QQ1,QQ2,'','')
            cursor.execute(sql)
            db.commit()

            #判断下一条message
            continue

        #枚举commments中的所有comment属性
        for comment in comments:
            try:
                #获取回复人qq(相对的)
                QQ1=comment['qq1']
                #获取被回复人qq(相对的)
                QQ2=comment['qq2']
            except:
                QQ1=comment['qq_1']
                QQ2=comment['qq_2']

            #获取comment的回复时间
            Reply_Time=comment['time_rep']
            #获取comment的回复内容
            Reply_Content=comment.text[:200]

            print PubTime
            #生成插入命令
            sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content.replace("'","\\'"),PubTime,QQ1,QQ2,Reply_Time,Reply_Content.replace("'",''))

            #执行插入命令
            try:
                cursor.execute(sql)
                db.commit()
            except Exception, e:
                db.rollback()
                print str(e),sql
                traceback.print_exc()



#处理留言板信息
def handleBoard(msgboard):

    #标记qq_info的类型
    feed_type='msgBoard'

    #获取所有的message标签
    boards=msgboard.findAll('message')



    #枚举所有的留言板信息
    for board in boards:
        try:
            #获取发布时间
            PubTime=board['time']

            #获取访问者的留言内容以及qq1
            content=board.findAll('content')[0].text[:200]
            QQ1=board['qq']
            QQ2=UserID

            #留言板的评论初始为空
            Reply_Content2 = ""
            Reply_Time2 = ""

            #获取留言板的所有评论
            comments = board.findAll('comments')[0].findAll('comment')

            #debug info
            print UserID + PubTime

            #如果没有评论
            if len(comments) == 0:
                sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content,PubTime,QQ1,QQ2,Reply_Time2,Reply_Content2.replace("'",''))
                cursor.execute(sql)
                db.commit()
                continue

            #枚举留言板的所有的评论
            for comment in comments:

                #获取回复人的qq
                QQ1=comment['qq1']
                #获取被回复人的qq
                QQ2=comment['qq2']
                #获取回复时间
                Reply_Time3=comment['time_rep']
                #获取回复内容
                Reply_Content3=comment.text[:200]

                #生成插入命令并执行
                sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content,PubTime,QQ1,QQ2,Reply_Time3,Reply_Content3.replace("'",''))
                cursor.execute(sql)

            db.commit()
        except:
            continue


#处理日志信息
def handleBlog(Blog):

    #标记qq_info的类型
    feed_type="Blog"

    #获取所有的blog标签
    Blogs=Blog.findAll('blog')

    #枚举所有的blog
    for blog in Blogs:

        try:

            #获取标题
            content=blog['title']
            #获取发布时间
            PubTime=blog['pubtime']

            #获取评论标签
            comments=blog.findAll('comments')[0]
            #获取所有的评论
            comments=comments.findAll('comment')

            #QQ1 QQ2初始化
            QQ1='00000000'
            QQ2='00000000'

            #回复时间初始化
            Reply_Time1 = ''

            print UserID+PubTime

            #如果评论数为0
            if len(comments)==0:
                sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content,PubTime,QQ1,QQ2,Reply_Time,Reply_Content.replace("'",''))
                cursor.execute(sql)
                db.commit()

                #判断已一条博客
                continue

            for comment in comments:

                #获取回复人的qq
                QQ1=comment['qq1']
                #获取被回复人的qq
                QQ2=comment['qq2']

                if len(QQ1) > 15 or len(QQ2) > 15:
                    continue

                #获取回复时间
                try:
                    Reply_Time1 = comment['time_rep']
                except :
                    Reply_Time1 = comment['posttime']

                #获取回复内容
                Reply_Content1=comment.text[:200]

                #生成插入命令并执行
                sql ="insert into qq_msg values('%s','%s','%s','%s','%s','%s','%s','%s')"%(UserID,feed_type,content,PubTime,QQ1,QQ2,Reply_Time1,Reply_Content1.replace("'",''))
                cursor.execute(sql)
            db.commit()
        except:
            continue


# 程序的主逻辑
for root,dishs,files in os.walk(filepath):

    SUM=len(files)

    bg = time.time()
    # 循环处理xml路径下的所有xml文件
    for xml in files:
        try:

            #生成xml filehandle
            f=file(root+'\\'+xml,'r').read()
            f=re.sub(r'(?m)^\s+','',f)
            f=re.sub(r'(?m)\s+$','',f)

            #通过filehandle生成beaufifulsoup对象
            soup = BeautifulSoup(f)

            #通过soup对象解析xml文件，将解析后的数据写入数据库
	    #print '看手机的反抗苦辣都来看过了'
            writeDB(soup)

        except Exception, e:
            print 'Error: %s' %str(e)
            print 'Error'+xml
            continue

        finally:
            SUM-=1
            print str(xml)+" is Finished,"+ str(SUM) +" left"
            
    end = time.time()
    print (end - bg)
    raw_input()
