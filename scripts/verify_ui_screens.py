import asyncio
import os
import subprocess
import sys
from playwright.async_api import async_playwright

async def capture_screens():
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
            
            # Poll /health until up
            server_up = False
            for _ in range(25):
                try:
                    res = await page.request.get("http://127.0.0.1:8000/health")
                    if res.status == 200:
                        server_up = True
                        break
                except Exception:
                    await asyncio.sleep(0.5)
            
            if not server_up:
                print("Server failed to respond on /health")
                return

            print("Server is up! Capturing New Chat screen...")
            await page.goto("http://127.0.0.1:8000/", wait_until="domcontentloaded")
            await asyncio.sleep(1.2)
            
            out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "assets"))
            os.makedirs(out_dir, exist_ok=True)
            
            # 1. New Chat Screen
            await page.screenshot(path=os.path.join(out_dir, "preview_new_chat.png"))
            print("Captured preview_new_chat.png")
            
            # 2. Observability Tab
            await page.evaluate("showView('observability')")
            await asyncio.sleep(0.8)
            await page.screenshot(path=os.path.join(out_dir, "preview_observability.png"))
            print("Captured preview_observability.png")

            # 3. Stats Tab
            await page.evaluate("showView('stats')")
            await asyncio.sleep(0.8)
            await page.screenshot(path=os.path.join(out_dir, "preview_stats.png"))
            print("Captured preview_stats.png")
            
            await browser.close()
    finally:
        server.terminate()
        server.wait()
        print("Server stopped.")

if __name__ == "__main__":
    asyncio.run(capture_screens())
