import asyncio
import os
import subprocess
import sys
import time
from playwright.async_api import async_playwright

async def capture():
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", "8000"],
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
                print("Failed to connect to gateway server.")
                return

            print("Connected to LLM Gateway! Navigating to /visualize...")
            # Use domcontentloaded because SSE traffic stream keeps network active indefinitely
            await page.goto("http://127.0.0.1:8000/visualize", wait_until="domcontentloaded")
            
            # Wait for elements
            await page.wait_for_selector("#studioCanvasViewport", timeout=10000)
            await page.wait_for_selector("#studioNodeGateway", timeout=10000)
            
            # Dismiss modal and show visualizer
            await page.evaluate("""() => {
                if (typeof closeAuthModal === 'function') closeAuthModal();
                const m = document.getElementById('authModal');
                if (m) m.remove();
                
                showView('visualize');
                
                // Sample active metrics in toolbar
                const vizInflight = document.getElementById('viz-router-inflight');
                if (vizInflight) vizInflight.innerText = '3';
                
                const now = performance.now();
                activeTrafficDots.set('req_viz_1', {
                    id: 'req_viz_1',
                    phase: 'ingress',
                    phaseStartTime: now - 350,
                    ingressDuration: 1400,
                    branchDuration: 750,
                    dwellDuration: 350,
                    fadeDuration: 600,
                    color: '#38bdf8',
                    filter: 'url(#glowBlue)',
                    provider: 'groq',
                    target: 'fast'
                });
                activeTrafficDots.set('req_viz_2', {
                    id: 'req_viz_2',
                    phase: 'ingress',
                    phaseStartTime: now - 900,
                    ingressDuration: 1400,
                    branchDuration: 750,
                    dwellDuration: 350,
                    fadeDuration: 600,
                    color: '#38bdf8',
                    filter: 'url(#glowBlue)',
                    provider: 'groq',
                    target: 'fast'
                });
                activeTrafficDots.set('req_viz_3', {
                    id: 'req_viz_3',
                    phase: 'branch',
                    phaseStartTime: now - 300,
                    ingressDuration: 1400,
                    branchDuration: 850,
                    dwellDuration: 350,
                    fadeDuration: 600,
                    color: '#10b981',
                    filter: 'url(#glowGreen)',
                    provider: 'groq',
                    target: 'fast'
                });
                activeTrafficDots.set('req_viz_4', {
                    id: 'req_viz_4',
                    phase: 'branch',
                    phaseStartTime: now - 500,
                    ingressDuration: 1400,
                    branchDuration: 900,
                    dwellDuration: 350,
                    fadeDuration: 600,
                    color: '#f59e0b',
                    filter: 'url(#glowAmber)',
                    provider: 'gemini',
                    target: 'smart'
                });
                
                if (typeof flashNodeStatus === 'function') {
                    flashNodeStatus('studioNodeClient', 'blue');
                    flashNodeStatus('studioNodeGateway', 'blue');
                    flashNodeStatus('studioNodeBalancer', 'blue');
                }
            }""")

            await asyncio.sleep(0.8)

            out_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "assets", "visualizer_traffic.png"))
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            await page.screenshot(path=out_path, full_page=False)
            print(f"Screenshot successfully captured at: {out_path}")
            
            await browser.close()
    finally:
        server.terminate()
        server.wait()
        print("Server stopped.")

if __name__ == "__main__":
    asyncio.run(capture())
