import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, null


url="https://portal.sw.nat.gov.tw/APGQO/GC331!downLoad?formBean.downLoadFile=CURRENT_JSON"
res = pd.read_json(url)
currency = pd.json_normalize(res['items'])

currency.columns=[u"cur_Id",u"cur_CashIn",u"cur_CashOut"]

currency[u"cur_NowIn"] = null

currency[u"cur_NowOut"] = null

currency[u"cur_Date"]= datetime.now().strftime("%Y-%m-%d")

currency[u"cur_Source"] = 'Portal'

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format('root', 'Idct1688', 'c6002.myds.me', '6055', 'C6002_EIP_Test','utf8'))
con = engine.connect()
currency.to_sql(name='currency', con=con, if_exists='append', index=False)