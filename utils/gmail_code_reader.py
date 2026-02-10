"""
Gmail API를 사용하여 TikTok 인증 코드를 자동으로 읽는 유틸리티.

Service Account + Domain-Wide Delegation 방식으로 Gmail에 접근하여
TikTok에서 발송한 인증 코드 이메일을 검색하고 6자리 코드를 추출합니다.
"""
import base64
import logging
import re
import time
from typing import Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

# Gmail API 스코프
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# TikTok 인증 이메일 발신자 패턴
TIKTOK_SENDER_QUERY = "(from:verify@tiktok.com OR from:noreply@tiktok.com)"

# 6자리 인증 코드 정규식
VERIFICATION_CODE_PATTERN = re.compile(r"\b(\d{6})\b")


class GmailVerificationCodeReader:
    """Gmail API를 사용하여 TikTok 인증 코드를 자동으로 읽는 유틸리티"""

    def __init__(
        self,
        service_account_file: str,
        delegated_user_email: str,
    ):
        """
        Args:
            service_account_file: Service Account JSON 키 파일 경로
            delegated_user_email: Domain-Wide Delegation 대상 사용자 이메일
        """
        self.service_account_file = service_account_file
        self.delegated_user_email = delegated_user_email
        self._service = None

    def _get_gmail_service(self):
        """Gmail API 서비스 객체 생성 (lazy initialization)"""
        if self._service is not None:
            return self._service

        credentials = service_account.Credentials.from_service_account_file(
            self.service_account_file,
            scopes=GMAIL_SCOPES,
        )
        delegated_credentials = credentials.with_subject(self.delegated_user_email)
        self._service = build("gmail", "v1", credentials=delegated_credentials)
        return self._service

    def test_connection(self) -> bool:
        """Gmail API 연결 테스트"""
        try:
            service = self._get_gmail_service()
            profile = service.users().getProfile(userId="me").execute()
            logger.info(f"Gmail API 연결 성공: {profile.get('emailAddress')}")
            return True
        except Exception as e:
            logger.error(f"Gmail API 연결 실패: {e}")
            return False

    def wait_for_verification_code(
        self,
        timeout: int = 120,
        poll_interval: int = 5,
    ) -> Optional[str]:
        """
        TikTok 인증 코드 이메일이 도착할 때까지 폴링하여 코드를 추출합니다.

        Args:
            timeout: 최대 대기 시간 (초)
            poll_interval: 폴링 간격 (초)

        Returns:
            6자리 인증 코드 문자열 또는 None (타임아웃)
        """
        start_time = time.time()
        # 검색 시작 시점의 epoch (초 단위) - 약간의 여유를 두고 10초 전부터 검색
        search_after_epoch = int(start_time) - 10

        logger.info(
            f"TikTok 인증 코드 이메일 대기 시작 "
            f"(최대 {timeout}초, {poll_interval}초 간격)"
        )

        while time.time() - start_time < timeout:
            code = self._search_verification_code(search_after_epoch)
            if code:
                logger.info(f"인증 코드 발견: {code}")
                return code

            elapsed = int(time.time() - start_time)
            logger.info(f"  인증 코드 대기 중... ({elapsed}초 경과)")
            time.sleep(poll_interval)

        logger.error(f"인증 코드 이메일 대기 타임아웃 ({timeout}초)")
        return None

    def _search_verification_code(self, after_epoch: int) -> Optional[str]:
        """
        Gmail에서 TikTok 인증 코드 이메일을 검색하고 코드를 추출합니다.

        Args:
            after_epoch: 이 시점 이후의 이메일만 검색 (Unix epoch 초)

        Returns:
            6자리 인증 코드 또는 None
        """
        try:
            service = self._get_gmail_service()

            # Gmail 검색 쿼리: TikTok 발신 + 최근 2분 이내
            query = f"{TIKTOK_SENDER_QUERY} newer_than:2m"

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
            for msg_ref in messages:
                msg = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_ref["id"], format="full")
                    .execute()
                )

                # 메시지 수신 시간 확인 (after_epoch 이후만)
                internal_date = int(msg.get("internalDate", "0")) // 1000
                if internal_date < after_epoch:
                    continue

                # 이메일 본문에서 코드 추출
                code = self._extract_code_from_message(msg)
                if code:
                    return code

            return None

        except Exception as e:
            logger.warning(f"Gmail 검색 오류: {e}")
            return None

    def _extract_code_from_message(self, message: dict) -> Optional[str]:
        """
        Gmail 메시지 본문에서 6자리 인증 코드를 추출합니다.

        Args:
            message: Gmail API 메시지 객체

        Returns:
            6자리 코드 문자열 또는 None
        """
        body_text = self._get_message_body(message)
        if not body_text:
            return None

        # 6자리 숫자 패턴 검색
        matches = VERIFICATION_CODE_PATTERN.findall(body_text)
        if matches:
            # 첫 번째 매치 반환 (보통 인증 코드가 가장 먼저 나옴)
            return matches[0]

        return None

    @staticmethod
    def _get_message_body(message: dict) -> str:
        """Gmail 메시지에서 본문 텍스트를 추출합니다."""
        payload = message.get("payload", {})
        parts = []

        def _extract_parts(payload_part: dict):
            mime_type = payload_part.get("mimeType", "")
            body = payload_part.get("body", {})
            data = body.get("data", "")

            if data and mime_type in ("text/plain", "text/html"):
                try:
                    decoded = base64.urlsafe_b64decode(data).decode("utf-8")
                    parts.append(decoded)
                except Exception:
                    pass

            # 멀티파트 메시지 처리
            for sub_part in payload_part.get("parts", []):
                _extract_parts(sub_part)

        _extract_parts(payload)
        return "\n".join(parts)
