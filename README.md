# 🚗 BlueSM Ride-Hailing Data Platform

> **Dự án Demo** — Hệ thống xử lý dữ liệu phân tích cho công ty vận tải gọi xe **BlueSM** (Blue Smart Mobility), xây dựng trên nền tảng **Databricks + dbt + PySpark** với kiến trúc Medallion (Bronze → Silver → Gold).

### 📊 Live Published Dashboard
🔗 **[Xem Databricks Live Dashboard — BlueSM Platform](https://dbc-1eb82f8a-7f14.cloud.databricks.com/dashboardsv3/01f19b9cdfac145c88ed22f8e4d89f7b/published?o=7474659662299061&f_operations_daily%7Echart_trips_trend=%257B%2522columns%2522%253A%255B%2522x%2522%255D%252C%2522rows%2522%253A%255B%255B%25222025-09-13T00%253A00%253A00.000Z%2522%255D%255D%257D)**

---

## 📋 Mục lục

1. [Tổng quan dự án](#1-tổng-quan-dự-án)
2. [Kiến trúc hệ thống](#2-kiến-trúc-hệ-thống)
3. [Luồng dữ liệu từ A đến Z](#3-luồng-dữ-liệu-từ-a-đến-z)
4. [Tầng Bronze — Dữ liệu thô](#4-tầng-bronze--dữ-liệu-thô)
5. [Tầng Silver — Dữ liệu làm sạch](#5-tầng-silver--dữ-liệu-làm-sạch)
6. [Tầng Gold — Dữ liệu phân tích](#6-tầng-gold--dữ-liệu-phân-tích)
7. [Snapshots — Quản lý lịch sử thay đổi (SCD Type 2)](#7-snapshots--quản-lý-lịch-sử-thay-đổi-scd-type-2)
8. [Semantic Layer — dbt MetricFlow](#8-semantic-layer--dbt-metricflow)
9. [CI/CD — Tự động hóa pipeline](#9-cicd--tự-động-hóa-pipeline)
10. [Dashboard — Databricks Lakeview](#10-dashboard--databricks-lakeview)
11. [Cấu trúc thư mục](#11-cấu-trúc-thư-mục)
12. [Hướng dẫn triển khai](#12-hướng-dẫn-triển-khai)
13. [Công nghệ sử dụng](#13-công-nghệ-sử-dụng)

---

## 1. Tổng quan dự án

### Bối cảnh

**BlueSM** là công ty vận tải gọi xe đang phát triển nhanh với hàng chục nghìn chuyến đi mỗi ngày trên nhiều thành phố. Dự án này xây dựng một nền tảng dữ liệu tập trung để:

- Theo dõi **doanh thu & vận hành** theo thời gian thực
- Phân tích **hiệu suất tài xế** và phân hạng Platinum/Gold/Silver
- Phân tích **hành vi khách hàng** theo mô hình RFM & Cohort Retention
- Tối ưu **tuyến đường & giờ cao điểm** để điều phối tài xế
- Đối soát **giao dịch thanh toán** theo từng cổng

### Phạm vi Demo

| Hạng mục | Chi tiết |
|---|---|
| **Nguồn dữ liệu** | Dữ liệu giả lập (synthetic) lưu trên Databricks Delta Lake |
| **Quy mô dataset** | ~10,000 chuyến đi, ~500 khách hàng, ~100 tài xế |
| **Tần suất cập nhật** | Mỗi ngày 1 lần (01:00 SA giờ Hà Nội) |
| **Dashboard** | Databricks Lakeview Dashboard (tích hợp sẵn) |
| **Triển khai** | GitHub Actions CI/CD tự động |

---

## 2. Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────┐
│                    BLUESM DATA PLATFORM                             │
│                                                                     │
│  ┌──────────────┐    ┌────────────────────────────────────────────┐ │
│  │   NGUỒN DL   │    │           DATABRICKS LAKEHOUSE             │ │
│  │              │    │                                            │ │
│  │  Hệ thống    │    │  ┌──────────┐  ┌────────────┐  ┌───────┐  │ │
│  │  vận hành    │───▶│  │  BRONZE  │─▶│   SILVER   │─▶│  GOLD │  │ │
│  │  (OLTP)      │    │  │ Raw Data │  │ Cleaned &  │  │ Data  │  │ │
│  │              │    │  │ Delta    │  │ Enriched   │  │ Marts │  │ │
│  │  customers   │    │  │ Tables   │  │ Delta      │  │       │  │ │
│  │  drivers     │    │  └──────────┘  └────────────┘  └───────┘  │ │
│  │  trips       │    │       ▲               ▲             ▲      │ │
│  │  payments    │    │       │               │             │      │ │
│  │  vehicles    │    │  PySpark Ingestion   dbt run       dbt run │ │
│  │  locations   │    │                                            │ │
│  └──────────────┘    │  ┌─────────────────────────────────────┐  │ │
│                       │  │        SNAPSHOTS (SCD Type 2)       │  │ │
│                       │  │  DimCustomers, DimDrivers,          │  │ │
│                       │  │  DimVehicles, DimPayments,          │  │ │
│                       │  │  DimLocations                       │  │ │
│                       │  └─────────────────────────────────────┘  │ │
│                       └────────────────────────────────────────────┘ │
│                                        │                             │
│  ┌─────────────────┐                   ▼                             │
│  │   GITHUB ACTIONS│    ┌──────────────────────────────────────┐    │
│  │   CI/CD         │    │     DATABRICKS LAKEVIEW DASHBOARD    │    │
│  │                 │    │  • Revenue & Operations Overview      │    │
│  │  Chạy tự động  │    │  • Driver Performance Analytics       │    │
│  │  01:00 SA/ngày │    │  • Customer Insights (RFM, Cohort)    │    │
│  └─────────────────┘    │  • Fleet & Payment Reconciliation    │    │
│                          └──────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
```

### Stack công nghệ

| Lớp | Công nghệ | Vai trò |
|---|---|---|
| **Storage** | Databricks Delta Lake | Lưu trữ dữ liệu theo định dạng ACID |
| **Ingestion** | PySpark (Databricks) | Nạp dữ liệu từ nguồn vào Bronze |
| **Transformation** | dbt-databricks 1.12.3 | Biến đổi Silver → Gold theo DAG |
| **SCD Type 2** | dbt Snapshot | Theo dõi lịch sử thay đổi Dim tables |
| **Semantic Layer** | dbt MetricFlow | Định nghĩa chuẩn metrics toàn dự án |
| **Orchestration** | GitHub Actions | Tự động chạy pipeline theo lịch |
| **Version Control** | Git + GitHub | Quản lý code và CI/CD |
| **Dashboard** | Databricks Lakeview | Trực quan hóa dữ liệu tầng Gold |

---

## 3. Luồng dữ liệu từ A đến Z

```
[NGUỒN]         [BRONZE]          [SILVER]           [GOLD]          [DASHBOARD]
   │                │                 │                  │                 │
   │  PySpark       │  dbt run        │  dbt run         │  Databricks     │
   │  Ingestion     │  (incremental)  │  (table)         │  Lakeview       │
   │                │                 │                  │                 │
   ▼                ▼                 ▼                  ▼                 ▼
Raw JSON/CSV ──▶ bronze.trips ──▶ silver.trips ──▶ gold.facttrip ──▶ Charts
                 bronze.customers ▶ silver.customers      ▶ gold.customer_daily_metric
                 bronze.drivers   ▶ silver.drivers        ▶ gold.driver_daily_metric
                 bronze.payments  ▶ silver.payments       ▶ gold.revenue_daily_metric
                 bronze.vehicles  ▶ silver.vehicles       ▶ gold.vehicle_daily_metric
                 bronze.locations ▶ silver.locations      ▶ gold.location_daily_metric
                                                          ▶ gold.payment_daily_reconciliation
                                                          ▶ gold.customer_rfm_segmentation_metric
                                                          ▶ gold.customer_cohort_retention_metric
                                                          ▶ gold.driver_productivity_efficiency_metric
                                                          ▶ gold.hourly_peak_demand_metric
                                                          ▶ gold.route_demand_profitability_metric
                                         ┌───────────────┘
                                    dbt Snapshot
                                    gold.DimCustomers (SCD2)
                                    gold.DimDrivers   (SCD2)
                                    gold.DimVehicles  (SCD2)
                                    gold.DimPayments  (SCD2)
                                    gold.DimLocations (SCD2)
```

**Thời gian xử lý mỗi lần chạy:**
- PySpark Ingestion (Bronze): ~2 phút
- dbt Snapshots (SCD2): ~1 phút
- dbt run Silver: ~2 phút (6 incremental models)
- dbt run Gold: ~3 phút (13 table models)
- dbt test: ~1 phút (14 tests)
- **Tổng**: ~10 phút/ngày

---

## 4. Tầng Bronze — Dữ liệu thô

**Vị trí**: `pysparkdbt.bronze.*`

**Mục đích**: Lưu trữ dữ liệu nguyên bản từ hệ thống vận hành, không biến đổi, giữ nguyên format gốc.

### Script ingestion: `pyspark_ingestion/`

| File | Chức năng |
|---|---|
| `bronze_ingestion.py` | Nạp dữ liệu thô từ nguồn vào Delta tables |
| `silver_transformation.py` | Biến đổi sơ bộ trước khi đưa vào Silver |
| `utils/custom_utils.py` | Các hàm tiện ích dùng chung |

### Cấu trúc 6 bảng Bronze

| Bảng | Các cột chính | Ghi chú |
|---|---|---|
| `bronze.customers` | `customer_id`, `first_name`, `last_name`, `email`, `phone_number`, `city`, `signup_date`, `last_updated_timestamp` | Thông tin khách hàng |
| `bronze.drivers` | `driver_id`, `first_name`, `last_name`, `phone_number`, `vehicle_id`, `driver_rating`, `city`, `last_updated_timestamp` | Thông tin tài xế |
| `bronze.trips` | `trip_id`, `customer_id`, `driver_id`, `vehicle_id`, `start_location`, `end_location`, `trip_start_time`, `trip_end_time`, `distance_km`, `fare_amount`, `payment_method`, `trip_status` | Giao dịch chuyến đi |
| `bronze.payments` | `payment_id`, `trip_id`, `customer_id`, `payment_method`, `payment_status`, `amount`, `transaction_time`, `last_updated_timestamp` | Giao dịch thanh toán |
| `bronze.vehicles` | `vehicle_id`, `license_plate`, `model`, `make`, `year`, `vehicle_type`, `last_updated_timestamp` | Thông tin phương tiện |
| `bronze.locations` | `location_id`, `city`, `state`, `country`, `latitude`, `longitude`, `last_updated_timestamp` | Danh mục địa điểm |

---

## 5. Tầng Silver — Dữ liệu làm sạch

**Vị trí**: `pysparkdbt.silver.*`

**Vật liệu hóa**: `incremental` (chỉ xử lý dữ liệu mới, không xử lý lại toàn bộ)

**Mục đích**: Chuẩn hóa kiểu dữ liệu, ghép tên, thêm các trường tính toán sơ bộ.

### Các biến đổi chính

| Model | Biến đổi đáng chú ý |
|---|---|
| `silver.customers` | Ghép `first_name + last_name` → `full_name`; Tách domain từ email → `domain` |
| `silver.drivers` | Ghép `first_name + last_name` → `full_name` |
| `silver.trips` | Chuẩn hóa `trip_status` viết hoa, lọc dữ liệu không hợp lệ |
| `silver.payments` | Thêm cờ `is_online_payment` (CARD hoặc WALLET = 1) |
| `silver.vehicles` | Chuẩn hóa tên hãng xe, loại xe |
| `silver.locations` | Chuẩn hóa tên thành phố, quốc gia |

### Data Tests (Kiểm tra chất lượng dữ liệu)

Mỗi bảng Silver đều có tests trong `models/silver/schema.yml`:

```yaml
# Ví dụ tests cho silver.trips
- unique: trip_id
- not_null: trip_id, customer_id, driver_id
```

**Kết quả**: 14/14 tests PASS ✅

---

## 6. Tầng Gold — Dữ liệu phân tích

**Vị trí**: `pysparkdbt.gold.*`

**Vật liệu hóa**: `table` (tạo lại hoàn toàn mỗi lần chạy)

**Mục đích**: Các bảng báo cáo sẵn sàng dùng cho Dashboard và phân tích BI.

### 6.1. Bảng Fact trung tâm

| Model | Mô tả |
|---|---|
| `gold.facttrip` | Bảng Fact các chuyến đi, kết hợp đầy đủ thông tin tài chính và vận hành |
| `gold.metricflow_time_spine` | Trục thời gian liên tục theo ngày, bắt buộc cho dbt MetricFlow |

### 6.2. Báo cáo theo Ngày (Daily Metrics)

| Model | Phân tích gì | Chiều Group by |
|---|---|---|
| `gold.revenue_daily_metric` | Doanh thu, tỷ lệ hoàn thành, revenue/km | `trip_date`, `payment_method` |
| `gold.customer_daily_metric` | Chi tiêu, số chuyến, phân loại thanh toán KH | `trip_date`, `customer_id` |
| `gold.driver_daily_metric` | Thu nhập, hiệu suất tài xế theo ngày | `trip_date`, `driver_id` |
| `gold.location_daily_metric` | Nhu cầu đón khách, doanh thu theo địa điểm | `trip_date`, `location_name` |
| `gold.vehicle_daily_metric` | Số xe active, doanh thu theo loại xe | `trip_date`, `vehicle_type` |
| `gold.payment_daily_reconciliation` | Đối soát giao dịch, tỷ lệ thành công | `transaction_date`, `payment_method`, `payment_status` |

### 6.3. Báo cáo Phân tích Nâng cao (Advanced Analytics)

| Model | Thuật toán / Phương pháp | Ứng dụng thực tế |
|---|---|---|
| `gold.customer_rfm_segmentation_metric` | RFM Score (Recency, Frequency, Monetary) + NTILE(5) | Phân hạng KH: CHAMPIONS/LOYAL/AT RISK/LOST |
| `gold.customer_cohort_retention_metric` | Cohort Analysis theo tháng đăng ký | Đo tỷ lệ giữ chân KH (Retention Rate) |
| `gold.driver_productivity_efficiency_metric` | Tổng hợp + phân hạng theo doanh thu + rating | Xếp hạng tài xế Platinum/Gold/Silver |
| `gold.hourly_peak_demand_metric` | Phân tích theo giờ (0-23h) + thứ trong tuần | Xác định giờ cao điểm điều phối tài xế |
| `gold.route_demand_profitability_metric` | Ma trận tuyến đường (Origin → Destination) | Tuyến đường lợi nhuận nhất |

### 6.4. Phân tích Tổng hợp Tài xế

| Model | Mô tả |
|---|---|
| `gold.driver_performance_metric` | Tổng hợp tích lũy toàn thời gian của từng tài xế |
| `gold.location_analytics_metric` | Tổng hợp doanh thu tích lũy theo địa điểm |

---

## 7. Snapshots — Quản lý lịch sử thay đổi (SCD Type 2)

**Vị trí**: `snapshots/SCDs.yaml`

**Vật liệu hóa**: Tự động thêm cột `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`

**Mục đích**: Lưu vết lịch sử thay đổi của các bảng Dimension, phục vụ phân tích lịch sử theo thời điểm (point-in-time analysis).

### 5 Dimension Tables với SCD Type 2

| Snapshot | Bảng nguồn | Cột theo dõi thay đổi | Ứng dụng |
|---|---|---|---|
| `DimCustomers` | `silver.customers` | `last_updated_timestamp` | Biết KH đã đổi thành phố/email khi nào |
| `DimDrivers` | `silver.drivers` | `last_updated_timestamp` | Biết tài xế đã thay xe/thay rating khi nào |
| `DimVehicles` | `silver.vehicles` | `last_updated_timestamp` | Biết xe đã đổi loại/năm SX khi nào |
| `DimPayments` | `silver.payments` | `last_updated_timestamp` | Lịch sử trạng thái giao dịch |
| `DimLocations` | `silver.locations` | `last_updated_timestamp` | Biết địa điểm đã thay đổi thông tin khi nào |

### Ví dụ cách đọc SCD Type 2

```sql
-- Lấy thông tin tài xế TẠI THỜI ĐIỂM ngày 2024-06-01
SELECT driver_id, full_name, driver_rating, city
FROM pysparkdbt.gold.DimDrivers
WHERE driver_id = 'D001'
  AND dbt_valid_from <= '2024-06-01'
  AND (dbt_valid_to > '2024-06-01' OR dbt_valid_to IS NULL);
```

---

## 8. Semantic Layer — dbt MetricFlow

**Vị trí**: `models/gold/semantic_models.yml`

**Mục đích**: Định nghĩa một tầng ngữ nghĩa chuẩn, giúp các công cụ BI tự động tính toán metrics mà không cần viết SQL.

### 5 Semantic Models

| Semantic Model | Nguồn | Số Measures |
|---|---|---|
| `metricflow_time_spine` | `gold.metricflow_time_spine` | — (trục thời gian) |
| `semantic_facttrip` | `gold.facttrip` | 10 measures |
| `semantic_customers` | `gold.DimCustomers` | 1 measure |
| `semantic_drivers` | `gold.DimDrivers` | 2 measures |
| `semantic_vehicles` | `gold.DimVehicles` | 2 measures |

### Ví dụ Measures được định nghĩa

```yaml
# Trong semantic_facttrip:
- total_revenue:        SUM(fare_amount)
- average_fare_per_trip: AVG(fare_amount)
- total_trips:          COUNT_DISTINCT(trip_id)
- total_completed_trips: COUNT_DISTINCT(trip_id) WHERE status=COMPLETED
- total_cancelled_trips: COUNT_DISTINCT(trip_id) WHERE status=CANCELLED
- total_unique_customers: COUNT_DISTINCT(customer_id)
- total_active_drivers:  COUNT_DISTINCT(driver_id)
- total_distance_km:    SUM(distance_km)
- average_trip_distance: AVG(distance_km)
```

---

## 9. CI/CD — Tự động hóa pipeline

**Vị trí**: `.github/workflows/`

### Workflow 1: Pipeline Sản xuất (`dbt_daily_pipeline.yml`)

```
Trigger 1: Mỗi ngày lúc 01:00 SA giờ Hà Nội (cron: "0 18 * * *" UTC)
Trigger 2: Khi push code mới vào nhánh main (thay đổi models/snapshots)
Trigger 3: Kích hoạt thủ công từ GitHub Actions UI

Quy trình thực thi (Ubuntu Runner trên GitHub):
  Bước 1:  📦 Checkout code từ GitHub
  Bước 2:  🐍 Cài Python 3.11
  Bước 3:  📚 Cài dbt-databricks==1.12.3
  Bước 4:  🔐 Tạo profiles.yml từ GitHub Secrets (không lưu token vào code)
  Bước 5:  🔌 dbt debug — Kiểm tra kết nối Databricks
  Bước 6:  📦 dbt deps — Cài packages (dbt_utils)
  Bước 7:  📸 dbt snapshot — Cập nhật 5 bảng Dim SCD Type 2
  Bước 8:  🥈 dbt run silver — Cập nhật 6 bảng Silver (incremental)
  Bước 9:  🥇 dbt run gold — Build 13 bảng Gold Data Marts
  Bước 10: 🧪 dbt test — Chạy 14 data quality tests
  Bước 11: 📄 dbt docs generate — Cập nhật Data Catalog
  Bước 12: 📤 Upload artifacts (catalog.json, manifest.json, index.html)
  Bước 13: 📬 Gửi thông báo kết quả qua Telegram
```

### Workflow 2: CI kiểm tra Pull Request (`dbt_ci_pr_check.yml`)

```
Trigger: Khi có Pull Request muốn merge vào nhánh main

Quy trình:
  Bước 1: Checkout code
  Bước 2: Cài Python & dbt-databricks
  Bước 3: Tạo profiles.yml
  Bước 4: dbt compile — Kiểm tra syntax SQL (không chạy trên Databricks)
  Bước 5: dbt test — Chạy data tests
  Bước 6: Tự động comment kết quả PASS/FAIL vào PR trên GitHub
```

### GitHub Secrets cần thiết

| Secret | Giá trị | Mục đích |
|---|---|---|
| `DATABRICKS_HOST` | `dbc-xxxx.cloud.databricks.com` | Địa chỉ Databricks workspace |
| `DATABRICKS_HTTP_PATH` | `/sql/1.0/warehouses/xxxx` | Đường dẫn SQL Warehouse |
| `DATABRICKS_TOKEN` | `dapi...` | Token xác thực (như mật khẩu) |
| `TELEGRAM_BOT_TOKEN` | `123456:ABCdef...` | Token bot Telegram (tùy chọn) |
| `TELEGRAM_CHAT_ID` | `987654321` | ID chat nhận thông báo (tùy chọn) |

---

## 10. Dashboard — Databricks Lakeview

**Công cụ**: Databricks SQL Dashboard (Lakeview)

🔗 **[Truy cập trực tiếp Published Dashboard](https://dbc-1eb82f8a-7f14.cloud.databricks.com/dashboardsv3/01f19b9cdfac145c88ed22f8e4d89f7b/published?o=7474659662299061&f_operations_daily%7Echart_trips_trend=%257B%2522columns%2522%253A%255B%2522x%2522%255D%252C%2522rows%2522%253A%255B%255B%25222025-09-13T00%253A00%253A00.000Z%2522%255D%255D%257D)**

**Truy cập Workspace**: Databricks → Menu trái → Dashboards

### Dashboard 1: Revenue & Operations Overview

Phân tích doanh thu tổng thể và hiệu suất vận hành:

| Chart | Bảng Gold | Chỉ số |
|---|---|---|
| KPI: Tổng doanh thu | `revenue_daily_metric` | `SUM(total_revenue)` |
| KPI: Tổng chuyến đi | `revenue_daily_metric` | `SUM(total_trips)` |
| KPI: Tỷ lệ hoàn thành | `revenue_daily_metric` | `AVG(completion_rate_pct)` |
| Line Chart: Doanh thu theo ngày | `revenue_daily_metric` | `total_revenue` theo `trip_date` |
| Bar Chart: Doanh thu theo phương thức TT | `revenue_daily_metric` | Group by `payment_method` |
| Heatmap: Giờ cao điểm | `hourly_peak_demand_metric` | `total_trips_demand` theo giờ x thứ |

### Dashboard 2: Driver Performance

Phân tích đội ngũ tài xế:

| Chart | Bảng Gold | Chỉ số |
|---|---|---|
| Bảng Top 20 tài xế | `driver_productivity_efficiency_metric` | `total_earnings`, `completion_rate_pct`, `driver_tier` |
| Donut: Phân bổ thứ hạng | `driver_productivity_efficiency_metric` | Platinum/Gold/Silver/Standard |
| Line: Thu nhập theo ngày | `driver_daily_metric` | `daily_total_earnings` theo `trip_date` |
| Bar: Rating theo thành phố | `driver_productivity_efficiency_metric` | `AVG(driver_rating)` theo `city` |

### Dashboard 3: Customer Insights

Phân tích hành vi khách hàng:

| Chart | Bảng Gold | Chỉ số |
|---|---|---|
| Treemap: Phân khúc RFM | `customer_rfm_segmentation_metric` | Số KH theo `rfm_segment` |
| Matrix: Cohort Retention | `customer_cohort_retention_metric` | `retention_rate_pct` theo tháng |
| Bar: ARPU theo phân khúc | `customer_rfm_segmentation_metric` | `AVG(monetary_spent)` theo segment |
| Line: Chi tiêu theo ngày | `customer_daily_metric` | `daily_total_spent` theo `trip_date` |

### Dashboard 4: Route & Location Analytics

Phân tích tuyến đường và địa điểm:

| Chart | Bảng Gold | Chỉ số |
|---|---|---|
| Bar: Top 15 tuyến đường | `route_demand_profitability_metric` | `total_route_revenue` |
| Scatter: Sản lượng vs Tỷ lệ HT | `route_demand_profitability_metric` | `total_route_trips` vs `route_completion_rate_pct` |
| Bar: Doanh thu theo địa điểm | `location_daily_metric` | `daily_location_revenue` theo `location_name` |

### Dashboard 5: Fleet & Payment Reconciliation

Phân tích đội xe và đối soát thanh toán:

| Chart | Bảng Gold | Chỉ số |
|---|---|---|
| Donut: Doanh thu theo loại xe | `vehicle_daily_metric` | `daily_fleet_revenue` theo `vehicle_type` |
| Bar: GD theo phương thức + trạng thái | `payment_daily_reconciliation` | `total_processed_amount` |
| Line: Khối lượng GD theo ngày | `payment_daily_reconciliation` | `total_transactions` theo `transaction_date` |

---

## 11. Cấu trúc thư mục

```
pysparkdbt/
├── .github/
│   └── workflows/
│       ├── dbt_daily_pipeline.yml      # CD: Pipeline tự động hàng ngày
│       └── dbt_ci_pr_check.yml         # CI: Kiểm tra Pull Request
│
├── models/
│   ├── sources/
│   │   └── sources.yml                 # Khai báo 3 nguồn: Bronze, Silver, Gold
│   ├── silver/
│   │   ├── customers.sql               # Incremental: làm sạch khách hàng
│   │   ├── drivers.sql                 # Incremental: làm sạch tài xế
│   │   ├── trips.sql                   # Incremental: làm sạch chuyến đi
│   │   ├── payments.sql                # Incremental: làm sạch thanh toán
│   │   ├── vehicles.sql                # Incremental: làm sạch phương tiện
│   │   ├── locations.sql               # Incremental: làm sạch địa điểm
│   │   └── schema.yml                  # Tests & docs tầng Silver
│   └── gold/
│       ├── facttrip.sql                # Bảng Fact trung tâm
│       ├── metricflow_time_spine.sql   # Trục thời gian MetricFlow
│       ├── revenue_daily_metric.sql    # Doanh thu theo ngày
│       ├── customer_daily_metric.sql   # KH theo ngày (có pay_card/cash/wallet)
│       ├── driver_daily_metric.sql     # Tài xế theo ngày
│       ├── location_daily_metric.sql   # Địa điểm theo ngày
│       ├── vehicle_daily_metric.sql    # Đội xe theo ngày
│       ├── payment_daily_reconciliation.sql    # Đối soát thanh toán
│       ├── customer_cohort_retention_metric.sql # Cohort Analysis
│       ├── customer_rfm_segmentation_metric.sql # RFM Scoring
│       ├── driver_productivity_efficiency_metric.sql # Phân hạng tài xế
│       ├── hourly_peak_demand_metric.sql        # Giờ cao điểm
│       ├── route_demand_profitability_metric.sql # Ma trận tuyến đường
│       ├── driver_performance_metric.sql        # Tổng hợp tài xế
│       ├── location_analytics_metric.sql        # Tổng hợp địa điểm
│       ├── schema.yml                  # Docs tầng Gold
│       └── semantic_models.yml         # dbt MetricFlow Semantic Layer
│
├── snapshots/
│   └── SCDs.yaml                       # 5 Dimension tables SCD Type 2
│
├── macros/
│   ├── generate_schema_name.sql        # Ép đúng schema bronze/silver/gold
│   └── drop_old_gold_tables.sql        # Dọn dẹp bảng cũ trên Databricks
│
├── analyses/
│   ├── mart_revenue_analytics.sql      # Ad-hoc query phân tích doanh thu
│   ├── mart_driver_performance.sql     # Ad-hoc query tài xế
│   ├── mart_customer_retention.sql     # Ad-hoc query giữ chân KH
│   ├── mart_location_analytics.sql     # Ad-hoc query địa điểm
│   └── mart_fleet_vehicle_analytics.sql # Ad-hoc query đội xe
│
├── pyspark_ingestion/
│   └── pyspark_dbt_project/
│       ├── bronze_ingestion.py         # Script PySpark nạp Bronze
│       ├── silver_transformation.py    # Script PySpark biến đổi Silver
│       └── utils/custom_utils.py       # Hàm tiện ích dùng chung
│
├── dbt_project.yml                     # Cấu hình dbt project
├── packages.yml                        # Khai báo dbt packages (dbt_utils)
└── README.md                           # Tài liệu dự án
```

---

## 12. Hướng dẫn triển khai

### Yêu cầu môi trường

- Python 3.11+
- Tài khoản Databricks (Community Edition hoặc cao hơn)
- SQL Warehouse đang hoạt động trên Databricks
- Tài khoản GitHub

### Bước 1: Clone repository

```powershell
git clone https://github.com/NgPcAnhh/pysparkdbt_study.git
cd pysparkdbt_study
```

### Bước 2: Tạo và kích hoạt môi trường ảo

```powershell
# Tạo venv
python -m venv .venv

# Kích hoạt (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Cài dbt-databricks
pip install dbt-databricks==1.12.3
```

### Bước 3: Cấu hình profiles.yml

Tạo file `C:\Users\{username}\.dbt\profiles.yml`:

```yaml
pysparkdbt:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: pysparkdbt
      schema: silver
      host: dbc-1eb82f8a-7f14.cloud.databricks.com
      http_path: /sql/1.0/warehouses/your_warehouse_id
      token: dapi_your_token_here
      threads: 4
```

### Bước 4: Kiểm tra kết nối

```powershell
dbt debug
```

### Bước 5: Cài packages

```powershell
dbt deps
```

### Bước 6: Chạy toàn bộ pipeline lần đầu

```powershell
# Chạy Snapshots (tạo 5 bảng Dim)
dbt snapshot

# Chạy toàn bộ models (Silver + Gold)
dbt run

# Kiểm tra chất lượng dữ liệu
dbt test

# Sinh tài liệu
dbt docs generate
dbt docs serve    # Mở http://localhost:8080
```

### Bước 7: Thiết lập CI/CD tự động

Xem chi tiết trong tài liệu [CI/CD Walkthrough](walkthrough.md):

1. Lấy `DATABRICKS_HOST`, `HTTP_PATH`, `TOKEN` từ Databricks
2. Vào `https://github.com/NgPcAnhh/pysparkdbt_study/settings/secrets/actions`
3. Thêm 3 GitHub Secrets
4. Test chạy tại `https://github.com/NgPcAnhh/pysparkdbt_study/actions`

### Bước 8: Tạo Dashboard trên Databricks

1. Vào Databricks → Menu trái → **Dashboards** → **Create Dashboard**
2. Đặt tên: `BlueSM Revenue & Operations`
3. Click **Add visualization** → Chọn **New query**
4. Copy và chạy các query phân tích từ thư mục `analyses/`
5. Chọn loại biểu đồ phù hợp → Lưu visualization
6. Lặp lại cho các chart khác

---

## 13. Công nghệ sử dụng

| Công nghệ | Phiên bản | Link |
|---|---|---|
| dbt-core | 1.12.0 | https://docs.getdbt.com |
| dbt-databricks | 1.12.3 | https://github.com/databricks/dbt-databricks |
| dbt-labs/dbt_utils | 1.3.0 | https://hub.getdbt.com/dbt-labs/dbt_utils |
| Apache Spark | 3.5+ (Databricks Runtime) | https://spark.apache.org |
| Delta Lake | 3.x (tích hợp Databricks) | https://delta.io |
| Python | 3.11 | https://python.org |
| GitHub Actions | — | https://docs.github.com/actions |

---

## 📬 Liên hệ & Đóng góp

- **Repository**: https://github.com/NgPcAnhh/pysparkdbt_study
- **Databricks Catalog**: `pysparkdbt`
- **Databricks Host**: `dbc-1eb82f8a-7f14.cloud.databricks.com`

> ⚠️ **Lưu ý**: Đây là dự án Demo với dữ liệu giả lập. Không sử dụng token hoặc thông tin bảo mật trong source code.
