#!/usr/bin/env python3
"""TikTok Seller Center 세션 쿠키 자동 갱신 스크립트.

로컬 Mac에서 실행하여 세션 쿠키를 갱신하고 EC2에 업로드합니다.
launchd 또는 cron으로 2일마다 자동 실행 권장.

사용법:
    python scripts/refresh_tiktok_session.py

환경변수 (.env 파일 또는 export):
    TIKTOK_EMAIL - TikTok 로그인 이메일
    TIKTOK_PASSWORD - TikTok 로그인 비밀번호
    EC2_HOST - EC2 서버 주소 (기본: 129.146.108.143)
    EC2_SSH_KEY - SSH 키 경로 (기본: ~/.ssh/oracle_cloud_oci)
"""

import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# 프로젝트 루트
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COOKIE_FILE = PROJECT_ROOT / "data" / "tiktok" / "tiktok_cookies.json"

# EC2 설정
EC2_HOST = os.getenv("EC2_HOST", "129.146.108.143")
EC2_USER = "ubuntu"
EC2_SSH_KEY = os.getenv("EC2_SSH_KEY", os.path.expanduser("~/.ssh/oracle_cloud_oci"))
EC2_COOKIE_PATH = "/home/ubuntu/scraper/data/tiktok/tiktok_cookies.json"

# TikTok 세션 쿠키 이름
SESSION_COOKIE_NAMES = {
    "sid_tt", "sessionid", "sessionid_ss", "sid_guard",
    "sid_tt_tiktokseller", "sessionid_tiktokseller",
    "sessionid_ss_tiktokseller", "sid_guard_tiktokseller",
}

SELLER_CENTER_URL = "https://seller-us.tiktok.com/homepage"
LOGIN_URL = "https://seller-us.tiktok.com/"


def load_env():
    """프로젝트 .env 파일에서 환경변수 로드."""
    env_file = PROJECT_ROOT / ".env"
    if not env_file.exists():
        # EC2에서 가져오기
        logger.info("로컬 .env 없음 → EC2에서 환경변수 가져오기")
        try:
            result = subprocess.run(
                ["ssh", "-i", EC2_SSH_KEY, f"{EC2_USER}@{EC2_HOST}",
                 "grep -E '^TIKTOK_(EMAIL|PASSWORD)=' /home/ubuntu/scraper/.env"],
                capture_output=True, text=True, timeout=10,
            )
            for line in result.stdout.strip().split("\n"):
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key, value)
        except Exception as e:
            logger.warning(f"EC2 환경변수 가져오기 실패: {e}")
        return

    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key, value)


def check_existing_session() -> dict | None:
    """기존 쿠키 파일의 세션 유효성 확인."""
    if not COOKIE_FILE.exists():
        return None

    try:
        with open(COOKIE_FILE) as f:
            cookies = json.load(f)

        session_cookies = [c for c in cookies if c["name"] in SESSION_COOKIE_NAMES]
        if not session_cookies:
            return None

        # 만료 시간 확인
        now = time.time()
        min_remaining = float("inf")
        for c in session_cookies:
            expires = c.get("expires", 0)
            if expires > 0:
                remaining_hours = (expires - now) / 3600
                min_remaining = min(min_remaining, remaining_hours)

        return {
            "count": len(session_cookies),
            "min_remaining_hours": min_remaining if min_remaining != float("inf") else 0,
        }
    except Exception:
        return None


def refresh_session() -> bool:
    """Playwright로 세션 쿠키 갱신."""
    from playwright.sync_api import sync_playwright

    email = os.getenv("TIKTOK_EMAIL", "")
    password = os.getenv("TIKTOK_PASSWORD", "")

    if not email or not password:
        logger.error("TIKTOK_EMAIL / TIKTOK_PASSWORD 환경변수가 필요합니다")
        return False

    COOKIE_FILE.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        # 브라우저 시작 (headed - 캡차 대응)
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1440, "height": 900},
            locale="en-US",
            timezone_id="America/New_York",
        )

        # 기존 쿠키 로드
        if COOKIE_FILE.exists():
            try:
                with open(COOKIE_FILE) as f:
                    existing_cookies = json.load(f)
                if existing_cookies:
                    context.add_cookies(existing_cookies)
                    logger.info(f"기존 쿠키 {len(existing_cookies)}개 로드")
            except Exception as e:
                logger.warning(f"기존 쿠키 로드 실패: {e}")

        page = context.new_page()

        # Seller Center 접속
        logger.info("Seller Center 접속 시도...")
        page.goto(SELLER_CENTER_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

        # 로그인 상태 확인
        current_url = page.url
        if "/account/login" in current_url or "/account/register" in current_url:
            logger.info("세션 만료 → 로그인 진행")
            success = _do_login(page, email, password)
            if not success:
                browser.close()
                return False
        elif "homepage" in current_url or "product/rating" in current_url:
            logger.info("기존 세션 유효! 쿠키 갱신만 진행")
        else:
            # 몇 초 더 대기
            page.wait_for_timeout(10000)
            current_url = page.url
            if "/account/login" in current_url or "/account/register" in current_url:
                logger.info("세션 만료 → 로그인 진행")
                success = _do_login(page, email, password)
                if not success:
                    browser.close()
                    return False

        # 쿠키 추출 및 저장
        cookies = context.cookies()
        tiktok_cookies = [c for c in cookies if "tiktok" in c.get("domain", "")]

        session_found = any(c["name"] in SESSION_COOKIE_NAMES for c in tiktok_cookies)
        if not session_found:
            logger.error("세션 쿠키를 찾을 수 없습니다")
            page.screenshot(path=str(PROJECT_ROOT / "data" / "tiktok" / "debug_refresh_failed.png"))
            browser.close()
            return False

        with open(COOKIE_FILE, "w") as f:
            json.dump(tiktok_cookies, f, indent=2)
        logger.info(f"쿠키 저장 완료: {len(tiktok_cookies)}개 → {COOKIE_FILE}")

        # 만료 시간 확인
        now = time.time()
        for c in tiktok_cookies:
            if c["name"] == "sessionid_tiktokseller":
                remaining_h = (c.get("expires", 0) - now) / 3600
                logger.info(f"세션 만료까지: {remaining_h:.1f}시간")

        browser.close()
        return True


def _do_login(page, email: str, password: str) -> bool:
    """Seller Center 로그인 수행."""
    current_url = page.url

    # 이미 로그인 페이지가 아니면 이동
    if "/account/login" not in current_url:
        page.goto(LOGIN_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)

    # SSO iframe 로그인 또는 직접 로그인
    try:
        # Log in 탭 찾기
        login_tab = page.query_selector('div:has-text("Log in") >> nth=0')
        if login_tab:
            login_tab.click()
            page.wait_for_timeout(2000)

        # 이메일 입력
        email_input = page.query_selector('input[name="email"], input[type="email"], input[placeholder*="email" i]')
        if email_input:
            email_input.fill(email)
            page.wait_for_timeout(500)

        # 비밀번호 입력
        pw_input = page.query_selector('input[name="password"], input[type="password"]')
        if pw_input:
            pw_input.fill(password)
            page.wait_for_timeout(500)

        # 로그인 버튼 클릭
        login_btn = page.query_selector('button:has-text("Log in"), button:has-text("로그인")')
        if login_btn:
            login_btn.click()
            logger.info("로그인 버튼 클릭")

        # 캡차 또는 2FA 대기 (수동 개입 필요할 수 있음)
        logger.info("로그인 처리 대기 중... (캡차/2FA가 나타나면 수동으로 완료해주세요)")
        for i in range(24):  # 최대 2분 대기
            page.wait_for_timeout(5000)
            url = page.url
            if "homepage" in url or "product/rating" in url:
                logger.info("로그인 성공!")
                return True
            if "/account/login" not in url and "/account/register" not in url:
                logger.info(f"로그인 진행 중... URL: {url[:80]}")

        logger.error("로그인 타임아웃")
        return False

    except Exception as e:
        logger.error(f"로그인 오류: {e}")
        return False


def upload_to_ec2() -> bool:
    """쿠키 파일을 EC2에 업로드."""
    if not COOKIE_FILE.exists():
        logger.error("업로드할 쿠키 파일이 없습니다")
        return False

    try:
        result = subprocess.run(
            ["scp", "-i", EC2_SSH_KEY,
             str(COOKIE_FILE),
             f"{EC2_USER}@{EC2_HOST}:{EC2_COOKIE_PATH}"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode == 0:
            logger.info(f"EC2 업로드 완료: {EC2_COOKIE_PATH}")
            return True
        else:
            logger.error(f"EC2 업로드 실패: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"EC2 업로드 오류: {e}")
        return False


def main():
    logger.info("=" * 60)
    logger.info("TikTok 세션 쿠키 갱신 시작")
    logger.info(f"시각: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    load_env()

    # 기존 세션 확인
    session = check_existing_session()
    if session:
        hours = session["min_remaining_hours"]
        logger.info(f"기존 세션: {session['count']}개 쿠키, 만료까지 {hours:.1f}시간")
        if hours > 24:
            logger.info("세션이 24시간 이상 유효 → 갱신 불필요, 스킵")
            return
        logger.info("세션 만료 임박 → 갱신 진행")
    else:
        logger.info("유효한 세션 없음 → 새로 로그인")

    # 세션 갱신
    success = refresh_session()
    if not success:
        logger.error("세션 갱신 실패!")
        sys.exit(1)

    # EC2 업로드
    uploaded = upload_to_ec2()
    if not uploaded:
        logger.warning("EC2 업로드 실패 - 수동으로 업로드 필요")

    logger.info("=" * 60)
    logger.info("세션 갱신 완료!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
