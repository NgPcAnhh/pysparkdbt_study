# pysparkdbt

Dự án dbt kết nối tới Databricks cho dự án `pysparkdbt`.

---

## Hướng dẫn sử dụng dbt cho những lần làm việc sau

### 1. Mở Terminal và Kích hoạt môi trường (Virtual Environment)
Mở PowerShell tại thư mục dự án `D:\project\pysparkdbt`.

> **Lưu ý:** Nếu PowerShell báo lỗi *Running scripts is disabled on this system*, chạy lệnh cấp quyền sau **1 lần duy nhất**:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

Kích hoạt môi trường ảo:
```powershell
D:\project\dbt_study\.venv\Scripts\Activate.ps1
```


### 2. Kiểm tra kết nối tới Databricks
Trước khi chạy code, kiểm tra xem kết nối tới Databricks có hoạt động tốt không:

```powershell
dbt debug
```

### 3. Các lệnh dbt thường dùng

- **Chạy tất cả các models:**
  ```powershell
  dbt run
  ```

- **Chạy một model cụ thể:**
  ```powershell
  dbt run --select <tên_model>
  # Ví dụ: dbt run --select 1
  ```

- **Chạy theo từng layer (bronze, silver, gold):**
  ```powershell
  dbt run --select bronze
  dbt run --select silver
  dbt run --select gold
  ```

- **Nạp dữ liệu từ file CSV trong thư mục `seeds/`:**
  ```powershell
  dbt seed
  ```

- **Biên dịch thử các file SQL (xem SQL render ở thư mục `target/`):**
  ```powershell
  dbt compile
  ```

- **Chạy kiểm thử data (Data tests):**
  ```powershell
  dbt test
  ```

---

## Cấu trúc thư mục dbt

- **`models/bronze/`**: Các bảng dữ liệu thô (Bronze layer - `table`)
- **`models/silver/`**: Các bảng làm sạch & biến đổi (Silver layer - `incremental`)
- **`models/gold/`**: Các bảng tổng hợp dữ liệu kinh doanh (Gold layer - `table`)
- **`macros/`**: Nơi chứa các hàm/macro tái sử dụng SQL
- **`seeds/`**: Nơi chứa các file CSV dữ liệu tĩnh
- **`snapshots/`**: Quản lý lịch sử thay đổi dữ liệu (SCD Type 2)

---

## Thông tin cấu hình Databricks
- **Profile Name**: `pysparkdbt`
- **Catalog**: `pysparkdbt`
- **Host**: `dbc-1eb82f8a-7f14.cloud.databricks.com`
- **File cấu hình Profile**: `C:\Users\Admin\.dbt\profiles.yml`
