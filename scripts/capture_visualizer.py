import asyncio
import os
import sys
from playwright.async_api import async_playwright

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1400, "height": 900})
        await page.goto("http://127.0.0.1:8000/visualize", wait_until="networkidle")
        await page.wait_for_selector("#studioNodeAuth")
        await page.wait_for_selector("#studioNodeRateLimit")
        await page.wait_for_selector("#studioNodeRedis")
        # Give SVG cables time to render
        await asyncio.sleep(1)
        out_dir = r"C:\Users\Cog\.gemini\antigravity\brain\504f0eff-eed4-449e-9852-21a5c8d7d2ee"
        os.makedirs(out_dir, exist_ok=True)
        screenshot_path = os.path.join(out_dir, "visualizer_security_nodes.png")
        await page.screenshot(path=screenshot_path)
        print(f"Screenshot successfully saved to: {screenshot_path}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
