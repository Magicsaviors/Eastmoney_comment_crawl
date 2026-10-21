import os
import pandas as pd
import pymysql
from pymysql.cursors import SSCursor

conn = pymysql.connect(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "eastmoney_comment"),
    charset="utf8mb4",
)
sql = """
SELECT stock_code,
       post_content,
       post_publish_time,
       read_count
FROM comment
where stock_code not in();
"""
out_dir = os.getenv("EXPORT_DIR", "./exports")
os.makedirs(out_dir, exist_ok=True)
chunk_size = 1000000  

with conn.cursor(cursor=SSCursor) as cur:
    cur.execute(sql)
    columns = [i[0] for i in cur.description]
    chunk_idx = 0
    while True:
        rows = cur.fetchmany(chunk_size)
        if not rows:
            break
        df_chunk = pd.DataFrame(rows, columns=columns)
        # 每个chunk单独文件，每个文件自带表头
        file_path = os.path.join(out_dir, f"comment_{chunk_idx}.csv")
        df_chunk.to_csv(
            file_path,
            mode="w",
            header=True,
            index=False,
            encoding="utf-8-sig",
            quoting=1
        )
        print(f"写入分片 {chunk_idx}, {len(df_chunk)} 行 -> {file_path}")
        chunk_idx += 1
conn.close()
print("全部分片导出完成")
