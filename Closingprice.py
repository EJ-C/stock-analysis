# Get all pages of closing price from first day to today from web
# Samsung Electronics PlotChart
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup
from matplotlib import pyplot as plt


# Get last page's number of Samsung Electronics
url = 'https://finance.naver.com/item/sise_day.nhn?code=005930&page=1'
with urlopen(url) as doc:
  html = BeautifulSoup(doc, 'lxml')
  pgrr = html.find('td', class_='pgRR')
  s = str(pgrr.a['href']).split('=')
  last_page =s[-1]

# Get whole page
df = pd.DataFrame()
sise_url = 'https://finance.naver.com/item/sise_day.nhn?code=005930'
for page in range(1, int(last_page)+1):
  page_url = '{}&page={}'.format(sise_url, page)
  df = df.append(pd.read_html(page_url, header=0)[0])

# Data preprocessing
df = df.dropna()
df = df.iloc[0:100]
df = df.sort_values(by='날짜')

# Datq Visualization as a chart
plt.title('Samsung Electronics (Closing Price)')
plt.xticks(rotation=45)
plt.plot(df['날짜'], df['종가'], 'co-')
plt.grid(color='gray', linestyle='--')
plt.show()