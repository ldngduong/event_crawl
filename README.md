# Wedding Event Crawler

Một hệ thống API (FastAPI) tự động cào dữ liệu đám cưới từ các nền tảng đăng ký tiệc cưới phổ biến (Zola, TheKnot, WithJoy, AppyCouple, SayIDo). 

Hệ thống sử dụng **Crawl4AI** để thu thập dữ liệu thô và **OpenRouter (MoE Model)** để trích xuất, phân tích và xuất ra thông tin sự kiện đám cưới (Tên cô dâu chú rể, Ngày giờ, Địa điểm tổ chức) dưới dạng JSON.

---

## Quickstart hiện tại

```bash
./run_dev.sh
```

Server chạy mặc định ở `http://localhost:8006`.

InternationalConferenceAlerts preview:

```bash
curl "http://localhost:8006/international-conference-alerts/events?search_url=https%3A%2F%2Finternationalconferencealerts.com%2Fconferences%3Fq%3Dtech%26country%3D%26month%3D&limit=10&source=auto"
```

Radius filter dùng `lat`, `lng`, `radius_km`; nếu event chưa có tọa độ, crawler sẽ thử gọi `EAGLE_GEOCODING_URL` hoặc mặc định `http://localhost:3001/api/v1/geocoding/address`.

```bash
curl "http://localhost:8006/international-conference-alerts/events?q=tech&limit=10&lat=40.7128&lng=-74.0060&radius_km=100"
```

## Yêu cầu hệ thống

- Python 3.11 trở lên
- Git
- Một API Key của **OpenRouter** (Đăng ký miễn phí tại [openrouter.ai](https://openrouter.ai/)).

---

## Hướng dẫn cài đặt (Setup)

**Bước 1: Clone dự án về máy**
```bash
git clone git@github.com:ldngduong/wedding_crawler.git
cd wedding_crawler
```

**Bước 2: Tạo môi trường ảo (Virtual Environment)**
Tạo môi trường ảo để không ảnh hưởng đến các thư viện hệ thống:
```bash
python -m venv venv
```
Kích hoạt môi trường ảo:
- Trên **Windows**: `venv\Scripts\activate`
- Trên **MacOS/Linux**: `source venv/bin/activate`

**Bước 3: Cài đặt thư viện cần thiết**
```bash
pip install -r requirements.txt
```

**Bước 4: Thiết lập file môi trường (.env)**
Tạo một file `.env` tại thư mục gốc của dự án (hoặc copy từ `.env.example` nếu có) và cấu hình key của OpenRouter:
```env
OPENROUTER_API_KEY=sk-or-v1-xxxxxxxxxxxxxxxxxxxx
```

**Bước 5: Cài đặt thư viện chạy trình duyệt Playwright (cho Crawl4AI)**
Lần đầu tiên sử dụng Crawl4AI, bạn cần cài đặt Playwright:
```bash
playwright install
```

---

## Hướng dẫn chạy dự án

Bạn có thể khởi động server API bằng 1 trong 2 cách sau:

**Cách 1: Chạy trực tiếp qua Python**
```bash
python main.py
```
*(Server sẽ chạy ở địa chỉ `http://localhost:8001`)*

**Cách 2: Chạy qua Uvicorn (có tính năng reload tự cập nhật code)**
```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

---

## Hướng dẫn sử dụng API

Sau khi server đã chạy, bạn có thể gọi API để tìm kiếm đám cưới theo địa điểm.

**Endpoint:** `GET /weddings/{location}`

**Ví dụ:** Tìm kiếm các đám cưới diễn ra ở London
Mở trình duyệt hoặc dùng Postman/cURL truy cập vào:
```text
http://localhost:8001/weddings/london
```

**Kết quả JSON trả về:**
```json
{
  "weddings": [
    {
      "couple_names": "Hannah & Myles",
      "date": "Friday, October 9, 2026",
      "time": null,
      "venue": null,
      "location": "London, England",
      "source_url": "https://www.zola.com/wedding/hannahandmyles2026/event"
    },
    ...
  ]
}
```

---

## Những lưu ý quan trọng (Troubleshooting)

### BẮT BUỘC: SỬ DỤNG CLOUDFLARE 1.1.1.1 (WARP)
**Để giảm thiểu lỗi và tuyệt đối tránh bị ban (khóa) địa chỉ IP thật của bạn, bạn PHẢI BẬT Cloudflare 1.1.1.1 (WARP) hoặc một VPN tương đương trong suốt quá trình chạy dự án.** Các trang đăng ký đám cưới (đặc biệt là Zola) có cơ chế chống Bot (Cloudflare/Akamai) cực kỳ gắt gao. Nếu bị lỗi 403 liên tục, hãy tắt đi bật lại WARP để lấy IP mới.

---

1. **Lỗi `TimeoutException` khi Search:**
   Hệ thống dùng DuckDuckGo/Mojeek/Brave để tìm kiếm link URL. Khi bạn bật Cloudflare 1.1.1.1, thỉnh thoảng bạn sẽ thấy log đỏ báo lỗi `TimeoutException` do bị dính Captcha ẩn. Điều này là hoàn toàn bình thường. Hệ thống đã được lập trình để tự động thử nghiệm các search engine dự phòng khác, bạn cứ yên tâm để cho bot tiếp tục chạy.
   
2. **Lỗi `403 Forbidden` khi Crawl:**
   Như đã đề cập ở trên, Zola và TheKnot chặn bot rất mạnh. Nếu thấy log báo `Blocked by anti-bot protection: HTTP 403`, có nghĩa là IP hiện tại của bạn đã bị từ chối. Hãy reset lại WARP để đổi IP.

3. **Sai địa điểm (Wrong Location):**
   Bot đã được trang bị AI để tự động loại bỏ (lọc) những sự kiện nhắc đến vị trí tìm kiếm nhưng thực chất lại tổ chức ở một nơi khác (VD: đi tuần trăng mật ở London, nhưng đám cưới ở Mỹ). Bạn có thể theo dõi quá trình lọc qua Log ở terminal (`"wrong_location": true`).
