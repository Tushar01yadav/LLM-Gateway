import asyncio
import os
from playwright.async_api import async_playwright

async def capture_light():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 880})
        
        out_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "docs", "assets"))
        os.makedirs(out_dir, exist_ok=True)

        print("Navigating to home...")
        await page.goto("http://127.0.0.1:8000/", wait_until="domcontentloaded")
        await asyncio.sleep(1)

        # Apply Light Theme
        await page.evaluate("""() => {
            if (typeof applyAppTheme === 'function') applyAppTheme('light');
            localStorage.setItem('app_theme', 'light');
            document.documentElement.classList.add('light-theme');
            document.body.classList.add('light-theme');
        }""")
        await asyncio.sleep(0.5)

        # 1. New Chat Screen (Light Theme)
        chat_path = os.path.join(out_dir, "preview_new_chat.png")
        await page.screenshot(path=chat_path)
        print(f"Captured: {chat_path}")

        # 2. Observability Tab (Light Theme)
        await page.evaluate("showView('observability')")
        await asyncio.sleep(0.8)
        obs_path = os.path.join(out_dir, "preview_observability.png")
        await page.screenshot(path=obs_path)
        print(f"Captured: {obs_path}")

        # 3. Stats Tab (Light Theme)
        await page.evaluate("showView('stats')")
        await asyncio.sleep(0.8)
        stats_path = os.path.join(out_dir, "preview_stats.png")
        await page.screenshot(path=stats_path)
        print(f"Captured: {stats_path}")

        # 4. Topology Visualizer (Light Theme)
        print("Navigating to /visualize...")
        await page.goto("http://127.0.0.1:8000/visualize", wait_until="domcontentloaded")
        await page.wait_for_selector("#studioCanvasViewport")
        await page.evaluate("""() => {
            if (typeof applyAppTheme === 'function') applyAppTheme('light');
            localStorage.setItem('app_theme', 'light');
            document.documentElement.classList.add('light-theme');
            document.body.classList.add('light-theme');
            showView('visualize');
            
            // Sample in-flight traffic
            const now = performance.now();
            activeTrafficDots.set('req_viz_1', {
                id: 'req_viz_1',
                phase: 'ingress',
                phaseStartTime: now - 350,
                ingressDuration: 1400,
                branchDuration: 750,
                dwellDuration: 350,
                fadeDuration: 600,
                color: '#0284c7',
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
                color: '#0284c7',
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
                color: '#059669',
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
                color: '#d97706',
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
        viz_path = os.path.join(out_dir, "visualizer_traffic.png")
        await page.screenshot(path=viz_path)
        print(f"Captured: {viz_path}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(capture_light())
