# Samsung Electronics Candle Chart (New version)
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
import mplfinance as mpf

# Get last page's number of Samsung Electronics
url = 'https://finance.naver.com/item/sise_day.nhn?code=005930&page=1'
with urlopen(url) as doc:
  html = BeautifulSoup(doc, 'lxml')
  pgrr = html.find('td', class_='pgRR')
  s = str(pgrr.a['href']).split('=')
  last_page = s[-1]

# Get whole page
df = pd.DataFrame()
sise_url = 'https://finance.naver.com/item/sise_day.nhn?code=005930'
for page in range(1, int(last_page) + 1):
  page_url = '{}&page={}'.format(sise_url, page)
  df = df.append(pd.read_html(page_url, header=0)[0])

# Data preprocessing
df = df.dropna()
df = df.iloc[0:5]
df = df.rename(columns={'날짜':'Date', '시가':'Open', '고가':'High', '저가':'Low', '종가':'Close', '거래량':'Volume'})
df = df.sort_values(by='Date')
df.index = pd.to_datetime(df.Date)
df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

# Data Visualization
mpf.plot(df, title='Samsung candle chart', type='candle')

# Data Visualization2 ( Customized Chart )
kwargs = dict(title='Samsung customized chart', type='candle',
  mav=(2, 4, 6), volume=True, ylabel='ohlc candles')
mc = mpf.make_marketcolors(up='r', down='b', inherit=True)
s = mpf.make_mpf_style(marketcolors=mc)
mpf.plot(df, **kwargs, style=s)