"""
Gmail IMAP을 사용하여 TikTok 인증 코드를 자동으로 읽는 유틸리티.

IMAP + App Password 방식으로 Gmail에 접근하여
TikTok에서 발송한 인증 코드 이메일을 검색하고 6자리 코드를 추출합니다.

Google Cloud Console 권한 불필요 - Python 표준 라이브러리만 사용.
"""
import email
import imaplib
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)

# TikTok 인증 이메일 발신자
TIKTOK_SENDERS = [
    "register@account.tiktok.com",
    "verify@tiktok.com",
    "noreply@tiktok.com",
]

# 6자리 인증 코드 정규식
VERIFICATION_CODE_PATTERN = re.compile(r"\b(\d{6})\b")


class GmailVerificationCodeReader:
    """Gmail IMAP을 사용하여 TikTok 인증 코드를 자동으로 읽는 유틸리티"""

    IMAP_SERVER = "imap.gmail.com"
    IMAP_PORT = 993

    def __init__(self, imap_email: str, imap_app_password: str):
        """
        Args:
            imap_email: Gmail 이메일 주소
            imap_app_password: Google App Password (2FA 활성화 후 생성)
        """
        self.imap_email = imap_email
        self.imap_app_password = imap_app_password

    def _connect(self) -> imaplib.IMAP4_SSL:
        """IMAP SSL 연결 및 로그인"""
        mail = imaplib.IMAP4_SSL(self.IMAP_SERVER, self.IMAP_PORT)
        mail.login(self.imap_email, self.imap_app_password)
        return mail

    def test_connection(self) -> bool:
        """IMAP 연결 테스트"""
        try:
            mail = self._connect()
            mail.select("INBOX", readonly=True)
            logger.info(f"Gmail IMAP 연결 성공: {self.imap_email}")
            mail.logout()
            return True
        except Exception as e:
            logger.error(f"Gmail IMAP 연결 실패: {e}")
            return False

    def wait_for_verification_code(
        self,
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> Optional[str]:
        """동기 버전 (폴백용)"""
        start_time = time.time()
        search_after = datetime.now(timezone.utc) - timedelta(seconds=30)

        logger.info(
            f"TikTok 인증 코드 이메일 대기 시작 "
            f"(최대 {timeout}초, {poll_interval}초 간격)"
        )

        while time.time() - start_time < timeout:
            code = self._search_verification_code(search_after)
            if code:
                logger.info(f"인증 코드 발견: {code}")
                return code

            elapsed = int(time.time() - start_time)
            logger.info(f"  인증 코드 대기 중... ({elapsed}초 경과)")
            time.sleep(poll_interval)

        logger.error(f"인증 코드 이메일 대기 타임아웃 ({timeout}초)")
        return None

    async def async_wait_for_verification_code(
        self,
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> Optional[str]:
        """비동기 버전: Playwright event loop를 블로킹하지 않음"""
        import asyncio

        start_time = time.time()
        search_after = datetime.now(timezone.utc) - timedelta(seconds=30)

        logger.info(
            f"TikTok 인증 코드 이메일 대기 시작 "
            f"(최대 {timeout}초, {poll_interval}초 간격)"
        )

        loop = asyncio.get_event_loop()
        while time.time() - start_time < timeout:
            code = await loop.run_in_executor(
                None, self._search_verification_code, search_after
            )
            if code:
                logger.info(f"인증 코드 발견: {code}")
                return code

            elapsed = int(time.time() - start_time)
            logger.info(f"  인증 코드 대기 중... ({elapsed}초 경과)")
            await asyncio.sleep(poll_interval)

        logger.error(f"인증 코드 이메일 대기 타임아웃 ({timeout}초)")
        return None

    def _search_verification_code(self, after: datetime) -> Optional[str]:
        """
        Gmail IMAP에서 TikTok 인증 코드 이메일을 검색하고 코드를 추출합니다.

        Args:
            after: 이 시점 이후의 이메일만 검색

        Returns:
            6자리 인증 코드 또는 None
        """
        mail = None
        try:
            mail = self._connect()
            mail.select("INBOX", readonly=True)

            # IMAP 검색: 오늘 날짜 이후 + TikTok 발신자
            # IMAP SINCE는 날짜 단위 (시간 불가)이므로 오늘 날짜로 검색
            date_str = after.strftime("%d-%b-%Y")

            for sender in TIKTOK_SENDERS:
                search_criteria = f'(FROM "{sender}" SINCE {date_str})'
                status, msg_ids = mail.search(None, search_criteria)

                if status != "OK" or not msg_ids[0]:
                    continue

                # 가장 최신 메시지부터 확인 (역순)
                id_list = msg_ids[0].split()
                for msg_id in reversed(id_list):
                    status, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if status != "OK":
                        continue

                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)

                    # 수신 시간 확인 (after 이후만)
                    msg_date = email.utils.parsedate_to_datetime(msg["Date"])
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    if msg_date < after:
                        continue

                    # 본문에서 코드 추출
                    body = self._get_message_body(msg)
                    code = self._extract_code(body)
                    if code:
                        mail.logout()
                        return code

            mail.logout()
            return None

        except Exception as e:
            logger.warning(f"Gmail IMAP 검색 오류: {e}")
            if mail:
                try:
                    mail.logout()
                except Exception:
                    pass
            return None

    @staticmethod
    def _get_message_body(msg: email.message.Message) -> str:
        """이메일 메시지에서 본문 텍스트를 추출합니다."""
        parts = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                if content_type in ("text/plain", "text/html"):
                    try:
                        charset = part.get_content_charset() or "utf-8"
                        payload = part.get_payload(decode=True)
                        if payload:
                            parts.append(payload.decode(charset, errors="replace"))
                    except Exception:
                        pass
        else:
            try:
                charset = msg.get_content_charset() or "utf-8"
                payload = msg.get_payload(decode=True)
                if payload:
                    parts.append(payload.decode(charset, errors="replace"))
            except Exception:
                pass

        return "\n".join(parts)

    @staticmethod
    def _extract_code(body: str) -> Optional[str]:
        """본문 텍스트에서 6자리 인증 코드를 추출합니다."""
        if not body:
            return None

        matches = VERIFICATION_CODE_PATTERN.findall(body)
        if matches:
            return matches[0]

        return None
