# Eastmoney Guba Comments

一个可复用的东方财富股吧公开评论采集 skill。它支持按股票代码和日期范围抓取公开帖子，将结果写入 MySQL，并提供大数据量 CSV 分片导出与缺失股票检查工具。

## 特性

- 异步抓取，多股票并发调度
- 按日期二分定位页面，减少无效请求
- MySQL 断点记录与重复写入保护
- 风控响应后的全局冷却与退避重试
- 可配置限速、并发、数据库和 Chrome 路径
- 配置模板已脱敏，不包含个人密码和本机路径

## 快速开始

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m playwright install chromium
Copy-Item .env.example .env
```

编辑 `.env`，至少填写真实的 `MYSQL_PASSWORD`，然后运行：

```powershell
python eastmoney_data.py
```

默认读取 `stock_code.txt`。生产运行建议复制一份自己的股票代码清单，并设置：

```text
STOCK_CODE_FILE=./my_stock_codes.txt
START_DATE=2024-01-01 00:00:00
CUTOFF_DATE=2020-01-01 00:00:00
```

## 导出与检查

导出 `Comment` 表：

```powershell
python data_extract.py
```

默认写入 `./exports`，也可以通过 `EXPORT_DIR` 调整。

检查代码清单与数据库的覆盖情况：

```powershell
python examine.py
```

## 数据字段

主要字段包括 `stock_code`、`post_id`、`post_content`、`post_publish_time`、`post_url`、`author_id`、阅读数、评论数、点赞数和转发数。爬虫首次运行会自动创建 `Comment` 和 `crawl_progress` 表。

## 运行边界

本项目只面向公开可访问数据。请控制请求频率，遇到验证页或访问限制时停止并等待，遵守东方财富的服务条款、适用法律和你的数据使用权限。项目不提供登录、验证码绕过或隐私数据采集功能。

## 目录

```text
.
├── SKILL.md
├── agents/openai.yaml
├── eastmoney_data.py
├── data_extract.py
├── examine.py
├── .env.example
├── requirements.txt
└── stock_code.txt
```

