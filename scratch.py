import httpx
from bs4 import BeautifulSoup
import asyncio

async def test_filters():
    # Thử gọi Conferank với các tham số query param thông dụng
    test_urls = [
        "https://www.conferank.com/conferences?location=Las+Vegas",
        "https://www.conferank.com/conferences?from=2026-08-01&to=2026-08-31",
        "https://www.conferank.com/conferences?q=Las+Vegas",
        "https://www.conferank.com/conferences?search=Las+Vegas",
    ]
    
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for url in test_urls:
            print(f"\n--- Thử URL: {url} ---")
            try:
                resp = await client.get(url)
                soup = BeautifulSoup(resp.text, 'html.parser')
                cards = soup.find_all('div', class_='conference-content')
                print(f"Số sự kiện tìm thấy: {len(cards)}")
                
                # In ra tên 2 sự kiện đầu để xem có đúng kết quả lọc không
                for card in cards[:2]:
                    title_tag = card.find('h3', class_='conference-title')
                    if title_tag:
                        print(" ->", title_tag.text.strip())
            except Exception as e:
                print("Lỗi:", e)

if __name__ == "__main__":
    asyncio.run(test_filters())
