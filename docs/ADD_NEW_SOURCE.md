# Hướng Dẫn Thêm Nguồn Dữ Liệu Mới (Add New Ingestor)

Tài liệu này hướng dẫn cách thêm một nguồn API mới (ví dụ: Shopee, Tiki, Lazada) vào hệ thống.

## I. Lựa Chọn Kiến Trúc (Architecture Decision)

Bạn có hai cách để triển khai nguồn dữ liệu mới (Ingestor). Mỗi cách có ưu/nhược điểm riêng về **Downtime** và **Tài nguyên (Container)**.

### Cách 1: Đa Container (Multi-Container) - **Khuyên dùng cho Zero Downtime**
Mỗi nguồn dữ liệu chạy trong một container riêng biệt.
- **Ưu điểm**:
  - **Zero Downtime (Tuyệt đối)**: Khi bạn deploy code mới cho Shopee, container Pancake KHÔNG bị khởi động lại.
  - **Dễ Debug**: Lỗi ở Shopee không làm crash tiến trình của Pancake.
  - **Scale độc lập**: Shopee nhiều đơn có thể tăng resource, Pancake ít đơn giảm resource.
- **Nhược điểm**: Nhiều container hơn (mỗi container tốn thêm khoảng 20-50MB RAM).
- **Cấu trúc**: `cmd/pancake_ingest`, `cmd/shopee_ingest`, v.v.

### Cách 2: Đơn Container (Single-Container / Monolith)
Gộp tất cả nguồn dữ liệu vào một chương trình duy nhất.
- **Ưu điểm**: Chỉ quản lý 1 container, 1 file config chung.
- **Nhược điểm**:
  - **Có Downtime khi Deploy**: Khi thêm code Shopee, bạn phải restart container -> Pancake cũng bị dừng trong vài giây khởi động lại (Tuy nhiên với worker background, vài giây thường không đáng kể).
  - **Rủi ro chung**: Code Shopee bị panic có thể làm crash cả luồng Pancake.
- **Cấu trúc**: `cmd/all_ingest` (chạy loop khởi tạo nhiều Connector).

---

## II. Các Bước Thực Hiện (Triển khai theo Cách 1 - Chuẩn hiện tại)

Giả sử bạn muốn thêm **Shopee**.

### Bước 1: Tạo thư mục CMD riêng
Copy thư mục `cmd/pancake_ingest` thành `cmd/shopee_ingest`.
```bash
cp -r cmd/pancake_ingest cmd/shopee_ingest
```

### Bước 2: Cập Nhật Config
Thêm các biến môi trường cần thiết cho Shopee vào `internal/config/server_config.go`.

```go
// internal/config/server_config.go
type Config struct {
    // ...
    Shopee ShopeeConfig
}

type ShopeeConfig struct {
    BaseURL string
    APIKey  string
}

// Hàm Load():
// Shopee: ShopeeConfig{ ... getEnv(...) }
```

### Bước 3: Implement Logic Ingest
Sửa file `cmd/shopee_ingest/main.go`. Bạn cần thay đổi 2 hàm quan trọng trong `connector.PollingConfig`:

1.  **`RequestBuilder`**: Sửa logic để build URL theo chuẩn Shopee API.
2.  **`ResponseParser`**: Sửa logic để đọc JSON response của Shopee.

```go
// cmd/shopee_ingest/main.go

// 1. Config cho Shopee
connectorConfig := connector.PollingConfig{
    // ...
    URL: cfg.Shopee.BaseURL,
    
    // Logic Request
    RequestBuilder: func(offset connector.Offset) (*connector.HTTPRequest, error) {
        // Logic tạo request Shopee
        // Ví dụ: sort theo update_time, phân trang bằng cursor
    },

    // Logic Parse
    ResponseParser: func(resp *connector.HTTPResponse) ([]connector.Record, error) {
        // Struct hứng JSON Shopee
        var body struct {
            Orders []ShopeeOrder `json:"orders"`
            NextCursor string    `json:"next_cursor"`
        }
        // Parse và convert sang connector.Record
    },
}
```

### Bước 4: Tạo Docker Image
Nếu dùng mô hình Đa Container, bạn có thể tái sử dụng `Dockerfile` gốc, chỉ cần thay đổi argument khi build hoặc tạo file Dockerfile riêng nếu cần cài thêm thư viện đặc thù.

**Cách đơn giản nhất (Dùng chung Dockerfile):**
Khi chạy docker, chỉ cần override command:
```yaml
# docker-compose.yml
services:
  pancake-ingest:
    build: .
    command: ["./pancake_ingest"]
    
  shopee-ingest:
    build: .
    # Cần sửa Makefile hoặc build script để build ra binary shopee_ingest
    command: ["./shopee_ingest"] 
```

### Bước 5: Cập Nhật Makefile
Thêm lệnh build cho service mới.

```makefile
build-shopee:
	go build -o shopee_ingest ./cmd/shopee_ingest/
```

---

## III. Mẹo cho "Single Container" (Nếu bạn vẫn muốn dùng 1 Container)

Nếu bạn muốn tiết kiệm container, hãy sửa `cmd/server/main.go` (hoặc tạo mới) để chạy **song song** các connector.

```go
func main() {
    // ... load config ...

    // 1. Tạo Connector Pancake
    pancakeConn, _ := connector.NewHTTPPollingConnector(pancakeConfig)
    
    // 2. Tạo Connector Shopee
    shopeeConn, _ := connector.NewHTTPPollingConnector(shopeeConfig)

    // 3. Chạy cả hai
    go pancakeConn.Start(ctx)
    go shopeeConn.Start(ctx)

    // Wait for shutdown signal...
    <-ctx.Done()
}
```
*Lưu ý: Cách này buộc phải restart cả 2 khi update code của 1 bên.*
