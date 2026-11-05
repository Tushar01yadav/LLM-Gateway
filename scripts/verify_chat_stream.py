import asyncio
import os
import subprocess
import sys
from playwright.async_api import async_playwright

async def capture_chat():
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000", "--no-proxy-headers"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    print("Starting uvicorn server...")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(viewport={"width": 1440, "height": 880})
            
            for _ in range(25):
                try:
                    res = await page.request.get("http://127.0.0.1:8000/health")
                    if res.status == 200:
                        break
                except Exception:
                    await asyncio.sleep(0.5)

            print("Navigating to home...")
            await page.goto("http://127.0.0.1:8000/", wait_until="domcontentloaded")
            await asyncio.sleep(1)

            # Type message into #heroChatInput and press Enter
            await page.fill("#heroChatInput", "Explain the circuit breaker pattern in two sentences.")
            await page.keyboard.press("Enter")
            
            # Wait for response or chatScreen
            await page.wait_for_selector("#chatScreen:not(.hidden)", timeout=15000)
            await asyncio.sleep(3)

            out_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "assets", "preview_active_chat.png"))
            await page.screenshot(path=out_path)
            print("Captured preview_active_chat.png successfully!")
            
            await browser.close()
    finally:
        server.terminate()
        server.wait()
        print("Server stopped.")

if __name__ == "__main__":
    asyncio.run(capture_chat())
