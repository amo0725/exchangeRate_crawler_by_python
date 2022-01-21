import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
load_dotenv()

username=os.getenv("USERNAME")
password=os.getenv("PASSWORD")
domain=os.getenv("DOMAIN")
port=os.getenv("PORT")
database=os.getenv("DATABASE")
encode=os.getenv("ENCODE")
table=os.getenv("TABLE")


url="https://rate.bot.com.tw/xrt?Lang=zh-TW"
res = pd.read_html(url)
df = res[0]

currency=df.iloc[:,:5]

currency.columns=[u"cur_Id",u"cur_CashIn",u"cur_CashOut",u"cur_NowIn",u"cur_NowOut"]

currency[u'cur_Id']=currency[u'cur_Id'].str.extract('\((\w+)\)')

dateTime = datetime.now().strftime("%Y-%m-%d")
currency[u"cur_Date"]= dateTime

source = 'TWBank'
currency[u"cur_Source"] = source

dfLen = len(currency)
currency.loc[dfLen] = ['TWD', 1,1,1,1,dateTime,source]

engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}?charset={}".format( username, password, domain, port, database,encode))
con = engine.connect()
currency.to_sql(name=table, con=con, if_exists='append', index=False)
