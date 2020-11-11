import pandas as pd

class DBUpadater:
  def read_krx_code(self):
    """Read Corporations from KRX's CSV file and return to Dataframe"""
    url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method='\
      'download&searchType=13'
    krx = pd.read_html(url, header=0)[0]
    krx = krx [['종목코드', '회사명']]
    krx = krx.rename(colums={'종목코드':'Code','회사명':'Company'})
    krx.code = krx.code.map('{:06d}'.format)
    return krx

