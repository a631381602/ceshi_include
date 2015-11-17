#coding:utf-8

import scrapy,re,urllib,os,time,sys
from ceshi_include.items import CeshiIncludeItem
import MySQLdb as mdb

reload(sys)
sys.setdefaultencoding('utf-8')

current_date = time.strftime('%Y-%m-%d',time.localtime(time.time()))

def search(req,html):
     text = re.search(req,html)
     if text:
         data = text.group(1)
     else:
         data = 'no'
     return data

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
    # "/job/gID/city-ID.html":"jobgIDcityID",
    "/dxjy/ID/":"dxjyID",
    "/jobs/ID/":"jobsID",
    "/interview/ID/":"interviewID",
    "/salary/ID/":"salaryID",
    "/trends/ID/":"trendsID",
    # "/recruit/ID/":"recruitID",
    "/company-ID-bonuses.html":"company_ID_mbonuses"
	}

con = mdb.connect(host="",user="",passwd="",db="seo_data",charset='utf8')
query_file = open('/Users/sunjian/Desktop/ceshi_include/query_file','w')

for k,v in filename_dict.items():

	if v == 'dxjyID':
		number = 10
	else:
		number = 20

	print "select * from %s order by rand() limit %s" % (v,number)

	with con:
		cur = con.cursor(mdb.cursors.DictCursor)
		cur.execute("select * from %s order by rand() limit %s" % (v,number) )
		rows = cur.fetchall()

		for row in rows:
			query = '%s,%s' % (row['name'],v)
			query_file.write(query + "\n")

query_file.close()

class DmozSpider(scrapy.Spider):
	name = "dmoz"
	allowed_domains = ["www.baidu.com"]

	os.system('cat /Users/sunjian/Desktop/ceshi_include/query_file|wc -l')

	start_urls = []
	for line in open('/Users/sunjian/Desktop/ceshi_include/query_file'):
		line = line.strip()

		word = line.split(',')[0]
		CLASS = line.split(',')[1]		

		url = 'http://www.baidu.com/s?wd=%s&rsv_spt=1&rsv_iqid=0xb1d0911d0000807f&issp=1&f=8&rsv_bp=0&rsv_idx=2&ie=utf-8&tn=baiduhome_pg&rsv_enter=1&rsv_sug3=29&rsv_sug1=16&rsv_sug2=0&inputT=5542&rsv_sug4=6502&class=%s' % (urllib.quote(word),urllib.quote(CLASS))
		start_urls.append(url)

	def __get_url_query(self, url):
		m =  re.search("wd=(.*?)&", url).group(1)
		return m

	def __get_url_class(self, url):
		m =  re.search("class=(.*)", url).group(1)
		return m

	def parse(self,response):
		query = urllib.unquote(self.__get_url_query(response.url))
		CLASS = urllib.unquote(self.__get_url_class(response.url))

		item = CeshiIncludeItem()

		html = response.body

		if '<p>很抱歉，没有找到与<span' in html or '没有找到该URL' in html:
			result = 0
		else:
			result = 1

		related = search(r'百度为您找到相关结果约(\d+)个',html)

		item['query'] = query
		item['result'] = result
		item['related'] = related
		item['CLASS'] = CLASS
 		item['date'] = current_date

		yield item

