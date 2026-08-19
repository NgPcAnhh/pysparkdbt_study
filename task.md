# Danh Sách Việc Cần Làm (TODO Checklist) - Project pysparkdbt

---

## 📌 Giai đoạn 1: Bổ sung Packages & Macro chuẩn hóa
- [ ] **Tạo file `packages.yml`**: Khai báo package `dbt-labs/dbt_utils`.
- [ ] **Chạy `dbt deps`**: Cài đặt các package vào dự án.
- [ ] **Cấu hình macro**: Thêm macro hỗ trợ (nếu có).

---

## 📌 Giai đoạn 2: Hoàn thiện các Models tầng Silver
- [ ] **Tạo model `models/silver/customers.sql`**: Làm sạch & biến đổi dữ liệu khách hàng.
- [ ] **Tạo model `models/silver/drivers.sql`**: Làm sạch & biến đổi dữ liệu tài xế.
- [ ] **Tạo model `models/silver/vehicles.sql`**: Làm sạch & biến đổi dữ liệu phương tiện.
- [ ] **Tạo model `models/silver/payments.sql`**: Làm sạch & biến đổi dữ liệu thanh toán.
- [ ] **Tạo model `models/silver/locations.sql`**: Làm sạch & biến đổi dữ liệu vị trí.
- [ ] **Kiểm tra chạy thử**: Lệnh `dbt run --select silver`.

---

## 📌 Giai đoạn 3: Bổ sung Data Testing & Documentation (schema.yml)
- [ ] **Tạo `models/silver/schema.yml`**: Định nghĩa test `unique`, `not_null` cho các bảng Silver.
- [ ] **Tạo `models/gold/schema.yml`**: Định nghĩa test `relationships` (khóa ngoại giữa `facttrip` và các bảng Dim).
- [ ] **Thêm `description`**: Tài liệu hóa mô tả cho các bảng và cột dữ liệu.
- [ ] **Kiểm tra chạy test**: Lệnh `dbt test`.

---

## 📌 Giai đoạn 4: Xây dựng các Gold Data Marts (Báo cáo KPI & Phân tích)
- [ ] **Tạo model `models/gold/gold_daily_revenue.sql`**: Tổng hợp doanh thu, số chuyến đi và trung bình cước theo ngày.
- [ ] **Tạo model `models/gold/gold_driver_performance.sql`**: Báo cáo tổng hợp hiệu suất và thu nhập của từng tài xế.
- [ ] **Tạo model `models/gold/gold_location_analytics.sql`**: Phân tích điểm đón/trả phổ biến và doanh thu theo khu vực.
- [ ] **Kiểm tra chạy thử**: Lệnh `dbt run --select gold`.

---

## 📌 Giai đoạn 5: Tự động hóa & Kết nối BI (Docs, CI/CD, Orchestration, BI)
- [ ] **Sinh tài liệu dbt**: Lệnh `dbt docs generate` và kiểm tra dbt lineage graph.
- [ ] **Cấu hình Lập lịch (Orchestration)**: Tạo workflow tự động chạy trên Databricks Workflows / Airflow.
- [ ] **Tạo BI Dashboard**: Kết nối schema Gold vào Databricks SQL Dashboard / Power BI.
