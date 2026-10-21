import os

import pandas as pd
import pymysql

# ========== 1.读取txt里的股票列表 ==========
with open("stock_code_new.txt","r",encoding="utf-8") as f:
    txt_codes = [line.strip() for line in f if line.strip()]

# ========== 2.读取数据库全部stock_code ==========
conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "eastmoney_comment"),
)
sql = "SELECT DISTINCT stock_code FROM Comment;"
db_codes = pd.read_sql(sql, conn)["stock_code"].tolist()
conn.close()

# ========== 3.找txt存在，但数据库没有的股票（缺失清单） ==========
missing = [code for code in txt_codes if code not in db_codes]

print(f"TXT总数量：{len(txt_codes)}")
print(f"数据库股票数量：{len(db_codes)}")
print(f"【数据库缺失的股票数量】：{len(missing)}")
if missing:
    print("缺失股票代码列表：")
    for c in missing:
        print(c)
    # 导出缺失清单到文件
    pd.DataFrame({"missing_code":missing}).to_csv("missing_stock.csv",index=False)
else:
    print("✅ 数据库已覆盖txt中全部股票！")
