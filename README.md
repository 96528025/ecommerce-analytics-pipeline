# E-Commerce Analytics Pipeline

An end-to-end data engineering project covering batch ETL, big data processing, and real-time stream processing on e-commerce data.

## Architecture

```
Raw CSV Files (9 tables)
        │
        ├──── Batch Path ─────────────────────────────────────────────┐
        │                                                              │
        │   v1: etl.py (pandas)          v2: spark_etl.py (PySpark)  │
        │        │                               │                     │
        │        ▼                               ▼                     │
        │   PostgreSQL                       Parquet files            │
        │   (star schema)               (output/parquet/)             │
        │        │                               │                     │
        │        └───────────┬───────────────────┘                    │
        │                    ▼                                         │
        │           SQL Analytics Queries                              │
        └─────────────────────────────────────────────────────────────┘

        ├──── Streaming Path ─────────────────────────────────────────┐
        │                                                              │
        │   kafka_producer.py  →  Kafka Topic  →  kafka_consumer.py  │
        │   (simulate orders)     olist-orders    (real-time GMV,    │
        │                         3 partitions     high-value alerts) │
        └─────────────────────────────────────────────────────────────┘
```

## Pipeline Evolution

| Version | File | Processing | Storage | Purpose |
|---------|------|------------|---------|---------|
| v1 | `etl.py` | pandas | PostgreSQL | Baseline batch ETL; straightforward and readable |
| v2 | `spark_etl.py` | PySpark | Parquet | Scalable batch ETL; industry-standard big data stack |
| v3 | `kafka_producer.py` + `kafka_consumer.py` | Kafka | Stream | Real-time order stream simulation and processing |

## Tech Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.13 |
| Batch Processing (v1) | pandas |
| Batch Processing (v2) | PySpark 4.x |
| Stream Processing (v3) | Apache Kafka 4.x |
| Database | PostgreSQL 16 |
| Big Data Storage | Parquet |
| ORM / Connector | SQLAlchemy + psycopg2 |
| Data Source | [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) |

## Dataset

The [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) contains ~100K orders from 2016–2018 across 9 relational tables:

- `olist_orders_dataset.csv` — order lifecycle and timestamps
- `olist_order_items_dataset.csv` — products and prices per order
- `olist_customers_dataset.csv` — customer location
- `olist_sellers_dataset.csv` — seller location
- `olist_products_dataset.csv` — product attributes and category
- `olist_order_payments_dataset.csv` — payment method and value
- `olist_order_reviews_dataset.csv` — customer review scores
- `olist_geolocation_dataset.csv` — zip code coordinates
- `product_category_name_translation.csv` — Portuguese → English category names

## Star Schema Design

**Fact Table**
- `fact_order_items` — one row per order item; stores measurable metrics (price, freight, payment value, review score)

**Dimension Tables**
- `dim_customers` — customer city and state
- `dim_products` — product category (English), weight, and dimensions
- `dim_sellers` — seller city and state
- `dim_date` — full date hierarchy (year, quarter, month, day, weekday)

## Setup & Run

### Prerequisites
- Python 3.x
- PostgreSQL 16
- Apache Kafka (`brew install kafka`)

### Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary pyspark kafka-python
```

### Create database
```bash
createdb olist_dw
```

### Download dataset
Download from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place all CSV files in the project root.

### Run v1 ETL (pandas → PostgreSQL)
```bash
python3 etl.py
```

Expected output:
```
=== Extract ===
  Reading CSV files...
=== Transform ===
=== Load ===
  ✓ dim_customers: 99,441 rows
  ✓ dim_products: 32,951 rows
  ✓ dim_sellers: 3,095 rows
  ✓ dim_date: 634 rows
  ✓ fact_order_items: 112,650 rows

Done! Star schema loaded into olist_dw.
```

### Run v2 ETL (PySpark → Parquet)
```bash
python3 spark_etl.py
```

Output is saved to `output/parquet/` as columnar Parquet files, with Spark SQL analytics printed to console.

### Run analytics queries
```bash
psql olist_dw -f queries.sql | cat
```

### Run Kafka streaming pipeline
Open two terminals:

**Terminal 1 — Consumer (start first):**
```bash
brew services start kafka
source venv/bin/activate
python3 kafka_consumer.py
```

**Terminal 2 — Producer:**
```bash
source venv/bin/activate
python3 kafka_producer.py
```

Expected consumer output:
```
Listening to topic 'olist-orders'...
  ⭐ High-value order: 6b860b35... $544.00 (1 items)
  ⭐ High-value order: f169bd68... $759.00 (1 items)
  [1,000 orders] GMV: $134,560.28 | Avg: $134.56 | High-value: 30
  ...
=== Final Summary ===
Total orders processed : 98,666
Total GMV              : $13,361,786.08
Average order value    : $135.42
High-value orders      : 3,201 (3.2%)
```

## Key Findings

**GMV Growth**
- Platform GMV grew ~10x from early 2017 to mid-2018
- November 2017 saw the highest single-month spike (7,451 orders) — consistent with Brazil's Black Friday

**Top Categories by Revenue**
- `health_beauty` ranked #1 with $1.26M revenue and a 4.14 average review score
- `watches_gifts` had the highest average order value among top categories

**Regional Distribution**
- São Paulo (SP) accounted for 41,375 orders — 3× more than Rio de Janeiro (RJ)
- Remote states (PA, CE) showed higher average order values, suggesting limited local supply

**Streaming Insights**
- ~3.2% of orders exceed $500 (high-value threshold)
- Real-time GMV tracking shows consistent $130–135 average order value across the stream

## Project Structure

```
ecommerce-pipeline/
├── etl.py                   # v1: pandas batch ETL → PostgreSQL
├── spark_etl.py             # v2: PySpark batch ETL → Parquet
├── kafka_producer.py        # v3: Kafka producer (simulates order stream)
├── kafka_consumer.py        # v3: Kafka consumer (real-time GMV + alerts)
├── queries.sql              # SQL analytics queries
├── output/parquet/          # Parquet output from spark_etl.py
│   ├── dim_customers/
│   ├── dim_products/
│   ├── dim_sellers/
│   ├── dim_date/
│   └── fact_order_items/
└── [CSV files from Kaggle]
```

---

# 电商数据分析 Pipeline（中文说明）

一个完整的数据工程项目，覆盖批处理 ETL、大数据处理、实时流处理三个层次。

## 项目架构

```
原始 CSV 文件（9张表）
        │
        ├──── 批处理路径 ──────────────────────────────────────────────┐
        │                                                              │
        │   v1: etl.py（pandas）      v2: spark_etl.py（PySpark）    │
        │        │                               │                     │
        │        ▼                               ▼                     │
        │    PostgreSQL                      Parquet 文件              │
        │   （星型数据仓库）             （output/parquet/）           │
        │        └───────────┬───────────────────┘                    │
        │                    ▼                                         │
        │             SQL 分析查询                                      │
        └─────────────────────────────────────────────────────────────┘

        ├──── 流处理路径 ──────────────────────────────────────────────┐
        │                                                              │
        │  kafka_producer.py  →  Kafka Topic  →  kafka_consumer.py   │
        │   （模拟实时下单）      olist-orders    （实时 GMV 统计、    │
        │                        3个分区          高价值订单预警）     │
        └─────────────────────────────────────────────────────────────┘
```

## 版本演进

| 版本 | 文件 | 处理引擎 | 存储格式 | 说明 |
|------|------|----------|----------|------|
| v1 | `etl.py` | pandas | PostgreSQL | 基础批处理 ETL，代码简洁易读 |
| v2 | `spark_etl.py` | PySpark | Parquet | 可扩展批处理，贴近真实大数据生产环境 |
| v3 | `kafka_producer/consumer.py` | Kafka | 实时流 | 模拟实时订单流，流式处理与监控 |

## 技术栈

| 层级 | 工具 |
|------|------|
| 编程语言 | Python 3.13 |
| 批处理（v1） | pandas |
| 批处理（v2） | PySpark 4.x |
| 流处理（v3） | Apache Kafka 4.x |
| 数据库 | PostgreSQL 16 |
| 大数据存储格式 | Parquet |
| 数据库连接 | SQLAlchemy + psycopg2 |
| 数据来源 | Olist 巴西电商公开数据集（Kaggle） |

## 数据集说明

Olist 数据集包含 2016–2018 年约 10 万条真实订单，共 9 张关联表，涵盖订单、客户、卖家、产品、支付、评价等完整电商业务链路。

## 星型模型设计

**事实表**：`fact_order_items`
- 粒度：每行对应一个订单项
- 指标：商品价格、运费、支付金额、评分

**维度表**：
- `dim_customers`：客户所在城市和州
- `dim_products`：产品品类（英文）、重量、尺寸
- `dim_sellers`：卖家所在城市和州
- `dim_date`：完整日期层级（年、季度、月、日、星期）

## 快速运行

```bash
# 1. 创建虚拟环境并安装依赖
python3 -m venv venv
source venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary pyspark kafka-python

# 2. 创建数据库
createdb olist_dw

# 3. 运行 v1 ETL（pandas → PostgreSQL）
python3 etl.py

# 4. 运行 v2 ETL（PySpark → Parquet）
python3 spark_etl.py

# 5. 运行 SQL 分析查询
psql olist_dw -f queries.sql | cat

# 6. 运行 Kafka 流处理（需要两个终端）
brew services start kafka
# 终端1：python3 kafka_consumer.py
# 终端2：python3 kafka_producer.py
```

## 主要数据洞察

- **GMV 增长**：平台 GMV 从 2017 年初到 2018 年中增长约 10 倍；2017 年 11 月因黑色星期五单月订单量达峰值
- **品类表现**：健康美妆（health_beauty）收入第一，达 $126 万，且评分高达 4.14 分
- **地区分布**：圣保罗州（SP）订单量占全平台约 37%，远超其他州
- **流处理洞察**：约 3.2% 的订单超过 $500 高价值阈值；实时 GMV 均值稳定在 $130–135
