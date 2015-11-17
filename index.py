#coding:utf-8


'''
readme

主程序  --->
1、从RDS中随机抽取要查询收录的url
2、运行scrapy爬虫模块，已url为query，爬取百度搜索结果，结果写入RDS
3、读取RDS中收录数据，计算每类url收录率，写入JS图表对应的存储表
'''

import MySQLdb as mdb
import sys,os,time
from email.mime.text import MIMEText
import smtplib

reload(sys)
sys.setdefaultencoding('utf-8')

'''爬虫抓取百度PC端搜索结果'''
os.system(" scrapy crawl dmoz ")

'''提取当日抓取的收录数据'''
mysql_time = time.strftime('%Y-%m-%d',time.localtime(time.time()))

filename_dict = {
    "gsmID":"gsmID",
    "gsoID":"gsoID",
    "gsxID":"gsxID",
    "gsxsIDcID":"gsxsIDcID",
    "gzxsID":"gzxsID",
    "gzxsIDcID":"gzxsIDcID",
    "gsmshID":"gsmshID",
    "gswenID":"gswenID",
    "/msh/gID-zID/":"mshgIDzID",
    "/gsmIDcID":"gsmIDcID",
    "gsrID":"gsrID",
    "gsrIDcID":"gsrIDcID",
    "plID":"plID",
    "/job/gID":"jobgID",
    "/job/gID/city-ID.html":"jobgIDcityID",
    "/dxjy/ID/":"dxjyID",
    "/jobs/ID/":"jobsID",
    "/interview/ID/":"interviewID",
    "/salary/ID/":"salaryID",
    "/trends/ID/":"trendsID",
    "/recruit/ID/":"recruitID",
    "/company-ID-bonuses.html":"company_ID_bonuses"
	}

con = mdb.connect(host="",user="",passwd="",db="seo_data",charset='utf8');
cur = con.cursor(mdb.cursors.DictCursor)

with con:
	cur.execute(" select result,CLASS from baidu_pc_include where `date` = '%s' " % mysql_time )
	rows = cur.fetchall()

	query_file = open('jieguo','w')

	for row in rows:
		line = '%s,%s' % (row['result'],row['CLASS'])
		query_file.write(line + "\n")

'''计算看准及竞品网站排名'''
result_dict = {}
for k,v in filename_dict.items() :

	number = int(os.popen(' cat jieguo|grep "%s"|wc -l ' % v).read().strip())
	inclusion = int(os.popen( 'cat jieguo|grep "%s"|egrep "^1,"|wc -l ' % v ).read().strip())

	if inclusion == 0:
		inclusion_ratio = 0
	else:
		inclusion_ratio = str(format(float(int(inclusion))/number,'.0%')).replace('%','')

	result_dict[v] = inclusion_ratio

'''排名结果写入mysql'''
sql = '''INSERT INTO baidu_pc_include_sql VALUES ("%s",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' % (
	mysql_time,
	result_dict['gsmID'],
	result_dict['gsoID'],
	result_dict['gsxID'],
	result_dict['gsxsIDcID'],
	result_dict['gzxsID'],
	result_dict['gzxsIDcID'],
	result_dict['gsmshID'],
	result_dict['gswenID'],
	result_dict['mshgIDzID'],
	result_dict['gsmIDcID'],
	result_dict['gsrID'],
	result_dict['gsrIDcID'],
	result_dict['plID'],
	result_dict['jobgID'],
	result_dict['jobgIDcityID'],
	result_dict['dxjyID'],
	result_dict['jobsID'],
	result_dict['interviewID'],
	result_dict['salaryID'],
	result_dict['trendsID'],
	result_dict['recruitID'],
	result_dict['company_ID_bonuses']
	)

print 'Import：%s' % sql
try:
	cur.execute(sql)
	con.commit()
	print 'done'
except:
    con.rollback()

con.close()


'''邮件发送'''
mailto_list=['']
mail_host=""  #设置服务器
mail_user=""    #用户名
mail_pass=""   #口令
mail_postfix=""  #发件箱的后缀
mail_title = "%s_收录查询进度报告" % resultname     #邮件发送标题

def send_mail(to_list,sub,content):
    me=""
    msg = MIMEText(content,_subtype='plain',_charset='gb2312')
    msg['Subject'] = sub
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user,mail_pass)
        server.sendmail(me, to_list, msg.as_string())
        server.close()
        return True
    except Exception, e:
        print str(e)
        return False

if __name__ == '__main__':
    if send_mail(mailto_list,mail_title,"查询完毕，请登陆RDS验收"):
        print "发送成功"
    else:
        print "发送失败"
