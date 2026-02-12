"""
Gmail에서 TikTok 인증 코드를 자동으로 읽는 유틸리티.

방법 1 (우선): Gmail API + Service Account (도메인 전체 위임)
방법 2 (폴백): IMAP + App Password

TikTok에서 발송한 인증 코드 이메일을 검색하고 6자리 코드를 추출합니다.
"""
import base64
import email
import email.message
import email.utils
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
    """Gmail에서 TikTok 인증 코드를 자동으로 읽는 유틸리티.

    Gmail API (Service Account) 우선, IMAP 폴백.
    """

    IMAP_SERVER = "imap.gmail.com"
    IMAP_PORT = 993

    def __init__(
        self,
        service_account_file: str = "",
        target_email: str = "",
        imap_email: str = "",
        imap_app_password: str = "",
    ):
        """
        Args:
            service_account_file: Google Service Account JSON 파일 경로
            target_email: 인증 코드를 받는 Gmail 주소 (Service Account가 impersonate)
            imap_email: Gmail 이메일 주소 (IMAP 폴백용)
            imap_app_password: Google App Password (IMAP 폴백용)
        """
        self.service_account_file = service_account_file
        self.target_email = target_email
        self.imap_email = imap_email
        self.imap_app_password = imap_app_password
        self._gmail_service = None

    # =========================================================================
    # Gmail API (Service Account)
    # =========================================================================

    def _get_gmail_service(self):
        """Gmail API 서비스 객체 생성 (캐싱)."""
        if self._gmail_service:
            return self._gmail_service

        try:
            from google.oauth2 import service_account
            from googleapiclient.discovery import build

            SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

            credentials = service_account.Credentials.from_service_account_file(
                self.service_account_file, scopes=SCOPES
            )

            # 도메인 전체 위임: target_email 사용자로 impersonate
            delegated_credentials = credentials.with_subject(self.target_email)

            self._gmail_service = build(
                "gmail", "v1", credentials=delegated_credentials
            )
            logger.info(f"Gmail API 서비스 생성 완료 (대상: {self.target_email})")
            return self._gmail_service

        except Exception as e:
            logger.error(f"Gmail API 서비스 생성 실패: {e}")
            return None

    def _search_code_via_api(self, after: datetime) -> Optional[str]:
        """Gmail API로 TikTok 인증 코드 검색."""
        service = self._get_gmail_service()
        if not service:
            return None

        try:
            # Gmail API 검색 쿼리: TikTok 발신자 + 최근 메시지
            senders_query = " OR ".join(f"from:{s}" for s in TIKTOK_SENDERS)
            after_epoch = int(after.timestamp())
            query = f"({senders_query}) after:{after_epoch}"

            results = (
                service.users()
                .messages()
                .list(userId="me", q=query, maxResults=5)
                .execute()
            )

            messages = results.get("messages", [])
            if not messages:
                return None

            # 가장 최신 메시지부터 확인
            for msg_meta in messages:
                msg = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_meta["id"], format="full")
                    .execute()
                )

                # 본문 추출
                body = self._extract_body_from_api_message(msg)
                code = self._extract_code(body)
                if code:
                    return code

            return None

        except Exception as e:
            logger.warning(f"Gmail API 검색 오류: {e}")
            return None

    @staticmethod
    def _extract_body_from_api_message(msg: dict) -> str:
        """Gmail API 메시지에서 본문 텍스트 추출."""
        parts_text = []

        def _walk_parts(payload):
            mime_type = payload.get("mimeType", "")
            body_data = payload.get("body", {}).get("data", "")

            if body_data and mime_type in ("text/plain", "text/html"):
                decoded = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
                parts_text.append(decoded)

            for part in payload.get("parts", []):
                _walk_parts(part)

        payload = msg.get("payload", {})
        _walk_parts(payload)

        return "\n".join(parts_text)

    # =========================================================================
    # IMAP (폴백)
    # =========================================================================

    def _connect_imap(self) -> imaplib.IMAP4_SSL:
        """IMAP SSL 연결 및 로그인"""
        mail = imaplib.IMAP4_SSL(self.IMAP_SERVER, self.IMAP_PORT)
        mail.login(self.imap_email, self.imap_app_password)
        return mail

    def _search_code_via_imap(self, after: datetime) -> Optional[str]:
        """Gmail IMAP에서 TikTok 인증 코드 이메일을 검색."""
        mail = None
        try:
            mail = self._connect_imap()
            mail.select("INBOX", readonly=True)

            date_str = after.strftime("%d-%b-%Y")

            for sender in TIKTOK_SENDERS:
                search_criteria = f'(FROM "{sender}" SINCE {date_str})'
                status, msg_ids = mail.search(None, search_criteria)

                if status != "OK" or not msg_ids[0]:
                    continue

                id_list = msg_ids[0].split()
                for msg_id in reversed(id_list):
                    status, msg_data = mail.fetch(msg_id, "(RFC822)")
                    if status != "OK":
                        continue

                    raw_email = msg_data[0][1]
                    msg = email.message_from_bytes(raw_email)

                    msg_date = email.utils.parsedate_to_datetime(msg["Date"])
                    if msg_date.tzinfo is None:
                        msg_date = msg_date.replace(tzinfo=timezone.utc)
                    if msg_date < after:
                        continue

                    body = self._get_imap_message_body(msg)
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

    # =========================================================================
    # 통합 인터페이스
    # =========================================================================

    def _search_verification_code(self, after: datetime) -> Optional[str]:
        """인증 코드 검색 (Gmail API 우선, IMAP 폴백)."""
        # 1순위: Gmail API (Service Account)
        if self.service_account_file and self.target_email:
            code = self._search_code_via_api(after)
            if code:
                return code

        # 2순위: IMAP (폴백)
        if self.imap_email and self.imap_app_password:
            code = self._search_code_via_imap(after)
            if code:
                return code

        return None

    def test_connection(self) -> bool:
        """연결 테스트."""
        # Gmail API 테스트
        if self.service_account_file and self.target_email:
            try:
                service = self._get_gmail_service()
                if service:
                    service.users().getProfile(userId="me").execute()
                    logger.info(f"Gmail API 연결 성공: {self.target_email}")
                    return True
            except Exception as e:
                logger.error(f"Gmail API 연결 실패: {e}")

        # IMAP 테스트
        if self.imap_email and self.imap_app_password:
            try:
                mail = self._connect_imap()
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
        """동기 버전."""
        start_time = time.time()
        search_after = datetime.now(timezone.utc) - timedelta(seconds=30)

        method = "Gmail API" if (self.service_account_file and self.target_email) else "IMAP"
        logger.info(
            f"TikTok 인증 코드 이메일 대기 시작 ({method}) "
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

        method = "Gmail API" if (self.service_account_file and self.target_email) else "IMAP"
        logger.info(
            f"TikTok 인증 코드 이메일 대기 시작 ({method}) "
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

    # =========================================================================
    # Helpers
    # =========================================================================

    @staticmethod
    def _get_imap_message_body(msg: email.message.Message) -> str:
        """IMAP 이메일 메시지에서 본문 텍스트를 추출."""
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
        """본문 텍스트에서 6자리 인증 코드를 추출."""
        if not body:
            return None

        matches = VERIFICATION_CODE_PATTERN.findall(body)
        if matches:
            return matches[0]

        return None
