#!/usr/bin/env python3
"""
TikTok Seller Center 세션 유지 Heartbeat.

12시간마다 실행하여 Seller Center를 방문, 세션 쿠키를 갱신합니다.
세션이 만료되기 전에 쿠키를 갱신하면 재로그인 + CAPTCHA를 피할 수 있습니다.

사용법:
    python scripts/session_heartbeat.py

Cron 설정 예시 (12시간마다):
    0 */12 * * * cd /home/ubuntu/scraper && source /home/ubuntu/airflow-venv/bin/activate && DISPLAY=:99 python scripts/session_heartbeat.py
"""
import asyncio
import atexit
import json
import logging
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from patchright.async_api import async_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

DATA_DIR = os.environ.get("TIKTOK_DATA_DIR", "data/tiktok")
COOKIE_FILE = os.path.join(DATA_DIR, "cookies.json")
PROFILE_DIR = os.path.join(DATA_DIR, "browser_profile")
SELLER_CENTER_URL = "https://seller-us.tiktok.com/homepage"

# GPU/DBus 크래시 방지 안정화 플래그
STABLE_BROWSER_ARGS = [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-software-rasterizer",
    "--disable-features=VizDisplayCompositor",
    "--disable-extensions",
    "--no-first-run",
                "--no-restore-session-state",
    "--no-default-browser-check",
    "--disable-blink-features=AutomationControlled",
]


def _cleanup_profile_locks(profile_dir: str) -> None:
    """브라우저 프로파일 잠금 파일 삭제."""
    profile_path = Path(profile_dir)
    for lock_file in profile_path.rglob("*.lock"):
        try:
            lock_file.unlink(missing_ok=True)
            logger.info(f"잠금 파일 삭제: {lock_file}")
        except Exception:
            pass
    singleton = profile_path / "SingletonLock"
    if singleton.exists():
        try:
            singleton.unlink(missing_ok=True)
            logger.info(f"잠금 파일 삭제: {singleton}")
        except Exception:
            pass


async def _launch_context(pw, profile_dir: str, use_headless: bool):
    """브라우저 컨텍스트 실행. 실패 시 잠금 파일 삭제 후 재시도."""
    launch_kwargs = dict(
        headless=use_headless,
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/133.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
        ignore_default_args=["--enable-automation"],
        args=STABLE_BROWSER_ARGS,
    )
    try:
        return await pw.chromium.launch_persistent_context(profile_dir, **launch_kwargs)
    except Exception as e:
        logger.warning(f"launch_persistent_context 실패: {e}")
        logger.warning("프로파일 잠금 파일 삭제 후 재시도...")
        _cleanup_profile_locks(profile_dir)
        return await pw.chromium.launch_persistent_context(profile_dir, **launch_kwargs)


def _kill_stale_chrome():
    """프로세스 종료 시 Chrome 정리 (atexit)."""
    try:
        subprocess.run(
            ['pkill', '-9', '-f', 'chromium.*tiktok'],
            capture_output=True, timeout=5,
        )
        subprocess.run(
            ['pkill', '-9', '-f', 'chrome.*tiktok'],
            capture_output=True, timeout=5,
        )
    except Exception:
        pass

atexit.register(_kill_stale_chrome)


async def heartbeat():
    """Seller Center를 방문하여 세션 쿠키를 갱신합니다."""
    logger.info("=" * 60)
    logger.info("TikTok 세션 Heartbeat 시작")
    logger.info("=" * 60)

    # 쿠키 파일 존재 여부 확인
    if not os.path.exists(COOKIE_FILE):
        logger.warning(f"쿠키 파일 없음: {COOKIE_FILE} - heartbeat 건너뜀")
        return False

    # 쿠키 나이 확인
    mtime = os.path.getmtime(COOKIE_FILE)
    age_hours = (time.time() - mtime) / 3600
    logger.info(f"쿠키 나이: {age_hours:.1f}시간")

    if age_hours > 72:
        logger.warning(f"쿠키가 {age_hours:.1f}시간 전 - 만료 가능성 높음 (재로그인 필요)")

    Path(PROFILE_DIR).mkdir(parents=True, exist_ok=True)

    # headless 모드 결정
    display = os.environ.get("DISPLAY", "")
    use_headless = not bool(display)

    pw = await async_playwright().start()

    try:
        ctx = await _launch_context(pw, PROFILE_DIR, use_headless)

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        # 기존 쿠키 복원
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        if cookies:
            await ctx.add_cookies(cookies)
            logger.info(f"쿠키 {len(cookies)}개 복원")

        # Seller Center 방문 (세션 갱신)
        logger.info(f"Seller Center 방문: {SELLER_CENTER_URL}")
        await page.goto(SELLER_CENTER_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(8000)

        current_url = page.url

        # 로그인 상태 확인
        if "/account/login" in current_url or "/account/register" in current_url:
            logger.warning(f"세션 만료됨 - 재로그인 필요. URL: {current_url}")
            await ctx.close()
            await pw.stop()
            return False

        # 쿠키 갱신 저장
        # NOTE: Rating 페이지 방문은 scraper에서 처리.
        # Heartbeat에서 rating 페이지를 방문하면 guard 토큰 rate-limit으로
        # 이후 scraper가 rating 페이지를 열 때 ttp_session_expire=1 오류 발생.
        new_cookies = await ctx.cookies()
        with open(COOKIE_FILE, "w", encoding="utf-8") as f:
            json.dump(new_cookies, f, ensure_ascii=False, indent=2)

        logger.info(f"쿠키 갱신 완료: {len(new_cookies)}개 → {COOKIE_FILE}")
        logger.info(f"세션 유효! URL: {current_url[:80]}")

        await ctx.close()
        await pw.stop()
        return True

    except Exception as e:
        logger.error(f"Heartbeat 실패: {e}")
        try:
            if 'ctx' in dir():
                await ctx.close()
        except Exception:
            pass
        try:
            await pw.stop()
        except Exception:
            pass
        return False


if __name__ == "__main__":
    success = asyncio.run(heartbeat())
    if success:
        logger.info("Heartbeat 성공")
    else:
        logger.warning("Heartbeat 실패 - 세션 만료 가능")
        sys.exit(1)
