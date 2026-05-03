# E-Commerce Analytics Pipeline

An end-to-end data engineering project that builds a star-schema data warehouse from raw e-commerce data, enabling multi-dimensional business analysis with SQL.

## Architecture

```
Raw CSV Files (9 tables)
        │
        ▼
  ETL Pipeline (Python + pandas)
        │
        ├── Extract   →  Read & validate raw CSVs
        ├── Transform →  Clean, join, build star schema
        └── Load      →  Write to PostgreSQL
                │
                ▼
        Data Warehouse (PostgreSQL)
        ┌─────────────────────────────────┐
        │         fact_order_items        │
        │  (112,650 rows · order grain)   │
        └──┬──────┬──────┬──────┬────────┘
           │      │      │      │
     dim_customers  dim_products  dim_sellers  dim_date
     (99,441)       (32,951)      (3,095)      (634)
                │
                ▼
        SQL Analytics Queries
```

## Pipeline Evolution

| Version | File | Processing | Storage | Why |
|---------|------|------------|---------|-----|
| v1 | `etl.py` | pandas | PostgreSQL | Baseline implementation; straightforward and readable |
| v2 | `spark_etl.py` | PySpark | Parquet | Scalable to large datasets; industry-standard big data stack |

Both versions produce the same star schema. v2 demonstrates how the pipeline would look in a real production environment where data volume exceeds single-machine memory.

## Tech Stack

| Layer | Tool |
|-------|------|
| Language | Python 3.13 |
| Data Processing (v1) | pandas |
| Data Processing (v2) | PySpark 4.x |
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

### Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate
pip install pandas sqlalchemy psycopg2-binary
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
pip install pyspark
python3 spark_etl.py
```

Output is saved to `output/parquet/` as columnar Parquet files, with Spark SQL analytics printed to console.

### Run analytics queries
```bash
psql olist_dw -f queries.sql | cat
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

**Shipping Efficiency**
- Freight-to-price ratio varies significantly by category, indicating opportunities for logistics optimization

## Project Structure

```
ecommerce-pipeline/
├── etl.py                              # v1: pandas ETL → PostgreSQL
├── spark_etl.py                        # v2: PySpark ETL → Parquet
├── queries.sql                         # Business analytics SQL queries
├── output/parquet/                     # Parquet output from spark_etl.py
│   ├── dim_customers/
│   ├── dim_products/
│   ├── dim_sellers/
│   ├── dim_date/
│   └── fact_order_items/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
├── olist_geolocation_dataset.csv
└── product_category_name_translation.csv
```

---

# 电商数据分析 Pipeline（中文说明）

一个完整的数据工程项目，将原始电商数据构建成星型结构数据仓库，支持多维度业务 SQL 分析。

## 项目架构

```
原始 CSV 文件（9张表）
        │
        ▼
  ETL Pipeline（Python + pandas）
        │
        ├── Extract（抽取）  → 读取并校验原始 CSV
        ├── Transform（转换）→ 清洗、关联、构建星型模型
        └── Load（加载）     → 写入 PostgreSQL
                │
                ▼
        数据仓库（PostgreSQL）
        ┌─────────────────────────────────┐
        │         fact_order_items        │
        │    事实表（11.2万行·订单粒度）    │
        └──┬──────┬──────┬──────┬────────┘
           │      │      │      │
     dim_customers  dim_products  dim_sellers  dim_date
      客户维度表     产品维度表     卖家维度表   日期维度表
```

## 版本演进

| 版本 | 文件 | 处理引擎 | 存储格式 | 说明 |
|------|------|----------|----------|------|
| v1 | `etl.py` | pandas | PostgreSQL | 基础实现，代码简洁易读 |
| v2 | `spark_etl.py` | PySpark | Parquet | 可扩展至大数据规模，贴近真实生产环境 |

两个版本产出相同的星型模型结构。v2 展示了当数据量超出单机内存时，pipeline 在真实生产环境中的形态。

## 技术栈

| 层级 | 工具 |
|------|------|
| 编程语言 | Python 3.13 |
| 数据处理（v1） | pandas |
| 数据处理（v2） | PySpark 4.x |
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
pip install pandas sqlalchemy psycopg2-binary pyspark

# 2. 创建数据库
createdb olist_dw

# 3. 运行 v1 ETL（pandas → PostgreSQL）
python3 etl.py

# 4. 运行 v2 ETL（PySpark → Parquet）
python3 spark_etl.py

# 5. 运行 SQL 分析查询
psql olist_dw -f queries.sql | cat
```

## 主要数据洞察

- **GMV 增长**：平台 GMV 从 2017 年初到 2018 年中增长约 10 倍；2017 年 11 月因黑色星期五单月订单量达峰值
- **品类表现**：健康美妆（health_beauty）收入第一，达 $126 万，且评分高达 4.14 分
- **地区分布**：圣保罗州（SP）订单量占全平台约 37%，远超其他州
- **物流效率**：不同品类运费占比差异显著，存在物流优化空间
