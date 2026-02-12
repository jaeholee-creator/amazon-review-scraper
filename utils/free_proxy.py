"""무료 프록시 로테이션 유틸리티.

TikTok 로그인 시 IP 차단/Rate Limit 우회 용도.
여러 무료 프록시 소스에서 프록시를 가져와 검증 후 반환.
"""

import asyncio
import logging
import random
import time
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# 무료 프록시 소스 (API key 불필요)
PROXY_SOURCES = [
    # ProxyScrape - HTTPS 지원, elite 익명성
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=5000&ssl=yes&anonymity=elite",
    # ProxyScrape - SOCKS5
    "https://api.proxyscrape.com/v2/?request=displayproxies&protocol=socks5&timeout=5000",
    # GitHub 기반 프록시 리스트
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies_anonymous/http.txt",
]

# 프록시 검증용 URL
TEST_URL = "https://httpbin.org/ip"
# TikTok 접근 가능 여부 테스트
TIKTOK_TEST_URL = "https://seller-us.tiktok.com/api/v1/health"


async def _fetch_from_source(session: aiohttp.ClientSession, url: str) -> list[str]:
    """단일 소스에서 프록시 목록 가져오기."""
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return []
            text = await resp.text()
            proxies = []
            for line in text.strip().split("\n"):
                line = line.strip()
                if line and ":" in line and not line.startswith("#"):
                    # ip:port 형식 검증
                    parts = line.split(":")
                    if len(parts) == 2 and parts[1].isdigit():
                        proxies.append(line)
            return proxies
    except Exception as e:
        logger.debug(f"프록시 소스 실패 ({url[:50]}...): {e}")
        return []


async def fetch_proxies(max_count: int = 100) -> list[str]:
    """여러 소스에서 프록시 목록 수집."""
    all_proxies = set()

    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_from_source(session, url) for url in PROXY_SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, list):
                all_proxies.update(result)

    proxy_list = list(all_proxies)
    random.shuffle(proxy_list)

    logger.info(f"무료 프록시 {len(proxy_list)}개 수집 완료")
    return proxy_list[:max_count]


async def validate_proxy(
    proxy: str,
    protocol: str = "http",
    timeout: int = 8,
) -> Optional[str]:
    """프록시가 실제 동작하는지 검증. 성공 시 외부 IP 반환."""
    proxy_url = f"{protocol}://{proxy}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                TEST_URL,
                proxy=proxy_url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    ip = data.get("origin", "")
                    logger.debug(f"프록시 유효: {proxy} → IP: {ip}")
                    return ip
    except Exception:
        pass
    return None


async def get_working_proxies(count: int = 5, timeout: int = 8) -> list[dict]:
    """검증된 동작 가능한 프록시 목록 반환.

    Returns:
        list of {"proxy": "ip:port", "protocol": "http", "ip": "external_ip"}
    """
    candidates = await fetch_proxies(max_count=200)
    if not candidates:
        logger.warning("프록시를 가져올 수 없음")
        return []

    working = []
    # 배치로 동시 검증 (10개씩)
    batch_size = 15

    for i in range(0, len(candidates), batch_size):
        if len(working) >= count:
            break

        batch = candidates[i:i + batch_size]
        tasks = [validate_proxy(p, timeout=timeout) for p in batch]
        results = await asyncio.gather(*tasks)

        for proxy, ip in zip(batch, results):
            if ip:
                working.append({
                    "proxy": proxy,
                    "protocol": "http",
                    "ip": ip,
                })
                if len(working) >= count:
                    break

        logger.info(f"프록시 검증 진행: {i + len(batch)}/{len(candidates)} 테스트, {len(working)}개 유효")

    logger.info(f"동작 가능 프록시 {len(working)}개 확보")
    return working


# 간단한 CLI 테스트
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    async def main():
        proxies = await get_working_proxies(count=5)
        for p in proxies:
            print(f"  {p['protocol']}://{p['proxy']} → IP: {p['ip']}")

    asyncio.run(main())
