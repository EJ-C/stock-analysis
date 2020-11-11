import pandas as pd
from bs4 import BeautifulSoup
import urllib, pymysql, calendar, time, json
from urllib.request import urlopen
from datetime import datetime
from threading import Timer
import pymysql

class DBUpdater:
  def __init__(self):
    """Constructor: Connecting MariaDB and Generate Stock Code Dictionary"""
    self.conn = pymysql.connect(host='localhost', user='root', password='5412',
     db='Investar', charset='utf8')

    with self.conn.cursor() as curs:
      sql =  """
      CREATE TABLE IF NOT EXISTS company_info (
	      code VARCHAR(20),
	      company VARCHAR(40),
	      last_update DATE,
	      PRIMARY KEY (code))
      """
      curs.execute(sql)
      sql = """
      CREATE TABLE IF NOT EXISTS daily_price (
	      code VARCHAR(20),
	      date DATE,
	      open BIGINT(20),
	      high BIGINT(20),
	      low BIGINT(20),
	      close BIGINT(20),
	      diff BIGINT(20),
	      volume BIGINT(20),
	      PRIMARY KEY (code, date))
      """
      curs.execute(sql)
    self.conn.commit()

    self.codes = dict()
    self.update_comp_info()

  def __del__(self):
    """Disconnected MariaDB"""
    self.conn.close()
  
  def read_krx_code(self):
    """Read Corporations from KRX's CSV file and return to Dataframe"""
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
        'download&searchType=13'
    krx = pd.read_html(url, header=0)[0]
    krx = krx[['종목코드', '회사명']]
    krx = krx.rename(colums={'종목코드': 'Code', '회사명': 'Company'})
    krx.code = krx.code.map('{:06d}'.format)
    return krx
  
  def update_comp_info(self):
    """Update stock code in company_info table and Save on Dictionary"""
    sql = "SELECT * FROM company_info"
    df = pd.read_sql(sql, self.conn)
    for idx in range(len(df)):
      self.codes[df['code'].values[idx]] = df['company'].values[idx]
  
    with self.conn.cursor() as curs:
      sql = "SELECT max(last_update) from company_info"
      curs.execute(sql)
      rs = curs.fetchone()
      today = datetime.today().strftime('%m-%d-%Y')

      if rs[0] == None or rs[0].strftime('%m-%d-%Y') < today:
        krx = self.read_krx_code()
        for idx in range(len(krx)):
          code = krx.code.values[idx]
          company = krx.company.values[idx]
          sql = f"REPLACE INTO company_info (code, company, last"\
            f"_update) VALUES ('{code}', '{company}', '{today}')"
          curs.execute(sql)
          self.codes[code] = company
          tmnow = datetime.now().strftime('%m-%d-%Y %H:%M')
          print(f"[{tmnow}] #{idx+1:04d} REPLACE INTO company_info "\
                  f"VALUES ({code}, {company}, {today})")
          self.conn.commit()
          print("'")

  def read_naver(self, code, company, pages_to_fetch):
    """Read stock price from Naver finance and return to Dataframe"""
    try:
      url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
      with urlopen(url) as doc:
        if doc is None:
          return None
        html = BeautifulSoup(doc, "lxml")
        pgrr = html.find("td", class_="pgRR")
        if pgrr is None:
          return None
        s = str(pgrr.a["href"]).split('=')
        lastpage = s[-1]
      df = pd.DataFrame()
      pages = min(int(lastpage), pages_to_fetch)
      for page in range(1, pages + 1):
        pg_url = '{}&page={}'.format(url, page)
        df = df.append(pd.read_html(pg_url, header=0)[0])
        tmnow = datetime.now().strftime('%m-%d-%Y %H:%M')
        print('[{}] {} ({}) : {:04d}/{:04d} pages are downloading...'.
            format(tmnow, company, code, page, pages), end="\r")
        df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
    except Exception as e:
      print('Exception occured :', str(e))
      return None
    return df

  def replace_into_db(self, df, num, code, company):
    """Stock price that are read from Naver finance replace to database"""
    with self.conn.cursor() as curs:
      for r in df.iteruples():
        sql = "REPLACE INTO daily_price VALUES ('{}', '{}', {}, {}"\
          ", {}, {}, {}, {})".format(code, r.date, r.open, r.high, r.low, 
          r.close, r.diff, r.volume)
      curs.execute(sql)
    self.conn.commit()
    print('[{}] #{:04d} {} ({}) : {} rows > REPLACE INTO daily_'
          'price [OK]'.format(datetime.now().strftime('%Y-%m-%d'
          ' %H:%M'), num+1, company, code, len(df)))

  def update_daily_price(self, pages_to_fetch):
    """Read coporations' stock price from Naver finance and update database"""
    for idx, code in enumerate(self.codes):
        df = self.read_naver(code, self.codes[code], pages_to_fetch)
        if df is None:
          continue
        self.replace_into_db(df, idx, code, self.codes[code])
  
  def execute_daily(self):
    """Update daily_price table when started and At 5:00PM"""
    self.update_comp_info()
    try:
         with open('config.json', 'r') as in_file:
             config = json.load(in_file)
             pages_to_fetch = config['pages_to_fetch']
    except FileNotFoundError:
         with open('config.json', 'w') as out_file:
             pages_to_fetch = 100
             config = {'pages_to_fetch': 1}
             json.dump(config, out_file)
    self.update_daily_price(pages_to_fetch)
    tmnow = datetime.now()
    lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
    if tmnow.month == 12 and tmnow.day == lastday:
         tmnext = tmnow.replace(year=tmnow.year+1, month=1, day=1,
                                hour=17, minute=0, second=0)
    elif tmnow.day == lastday:
         tmnext = tmnow.replace(month=tmnow.month+1, day=1, hour=17,
                                minute=0, second=0)
    else:
         tmnext = tmnow.replace(day=tmnow.day+1, hour=17, minute=0,
             second=0)
    tmdiff = tmnext - tmnow
    secs = tmdiff.seconds
    t = Timer(secs, self.execute_daily)
    print("Waiting for next update ({}) ... ".format(tmnext.strftime
          ('%Y-%m-%d %H:%M')))
    t.start()

if __name__== '__main__':
  dbu = DBUpdater()
  dbu.execute_daily()
