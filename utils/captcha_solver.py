"""
TikTok 슬라이더 퍼즐 캡차 자동 풀기.

TikTok Seller Center 로그인 시 나타나는 원형 퍼즐 캡차를 자동으로 풀어줍니다.
배경 이미지의 갭 위치를 탐지하고 슬라이더를 인간처럼 드래그합니다.
"""
import asyncio
import io
import logging
import math
import random
from typing import Optional

from PIL import Image, ImageFilter

logger = logging.getLogger(__name__)


class TikTokCaptchaSolver:
    """TikTok 슬라이더 퍼즐 캡차 자동 풀기"""

    MAX_RETRIES = 3

    # DOM 셀렉터
    CONTAINER_SEL = ".captcha-verify-container"
    BG_IMAGE_SEL = 'img[class*="cap-h-"][class*="sm:cap-h-"]:not([class*="cap-absolute"])'
    PIECE_IMAGE_SEL = "img.cap-absolute"
    SLIDER_TRACK_SEL = ".cap-rounded-full.cap-bg-UISheetGrouped3"
    SLIDER_BUTTON_SEL = ".cap-rounded-full.cap-bg-UISheetGrouped3 .cap-absolute"
    REFRESH_SEL = "#captcha_refresh_button"
    CLOSE_SEL = "#captcha_close_button"

    def __init__(self, page):
        self.page = page

    async def is_captcha_visible(self) -> bool:
        """슬라이더 캡차가 화면에 표시되어 있는지 확인 (슬라이더 트랙 존재 여부로 판단)"""
        # 컨테이너가 아닌 슬라이더 트랙으로 확인 (인증 코드 페이지와 구분)
        slider = await self.page.query_selector(self.SLIDER_TRACK_SEL)
        if slider:
            return True
        # 퍼즐 이미지로도 확인
        bg_img = await self.page.query_selector(self.BG_IMAGE_SEL)
        return bg_img is not None

    async def solve(self) -> bool:
        """캡차 풀기 (최대 MAX_RETRIES번 시도)"""
        for attempt in range(1, self.MAX_RETRIES + 1):
            if not await self.is_captcha_visible():
                logger.info("캡차가 없음 - 통과")
                return True

            logger.info(f"캡차 풀기 시도 {attempt}/{self.MAX_RETRIES}")

            try:
                success = await self._attempt_solve()

                if not success:
                    # 요소를 못 찾은 경우 - 이미 캡차가 사라졌을 수 있음
                    await asyncio.sleep(1)
                    if not await self.is_captcha_visible():
                        logger.info("캡차 풀기 성공! (요소 소멸 확인)")
                        return True
                    logger.info("캡차 풀기 실패 - 리프레시 후 재시도")
                    await self._refresh()
                    await asyncio.sleep(2)
                    continue

                # 드래그 후 서버 검증 대기 (점진적 확인)
                for wait_i in range(5):
                    await asyncio.sleep(1)
                    if not await self.is_captcha_visible():
                        logger.info(f"캡차 풀기 성공! ({wait_i + 1}초 후 확인)")
                        return True

                logger.info("드래그 후 캡차 여전히 표시 - 리프레시 후 재시도")
                await self._refresh()
                await asyncio.sleep(2)

            except Exception as e:
                logger.warning(f"캡차 풀기 시도 {attempt} 오류: {e}")
                await asyncio.sleep(1)
                if not await self.is_captcha_visible():
                    return True
                await self._refresh()
                await asyncio.sleep(2)

        logger.error(f"캡차 풀기 최종 실패 ({self.MAX_RETRIES}번 시도)")
        return False

    async def _attempt_solve(self) -> bool:
        """단일 캡차 풀기 시도"""
        page = self.page

        # 1. 슬라이더 버튼 찾기
        slider_button = await page.query_selector(self.SLIDER_BUTTON_SEL)
        slider_track = await page.query_selector(self.SLIDER_TRACK_SEL)

        if not slider_button or not slider_track:
            logger.warning("슬라이더 요소를 찾을 수 없음")
            return False

        track_box = await slider_track.bounding_box()
        button_box = await slider_button.bounding_box()
        if not track_box or not button_box:
            return False

        track_width = track_box["width"]
        button_width = button_box["width"]
        usable_width = track_width - button_width

        # 2. 퍼즐 이미지 분석하여 타겟 위치 계산
        target_ratio = await self._analyze_puzzle_images()

        if target_ratio is not None:
            drag_distance = int(usable_width * target_ratio)
            logger.info(f"이미지 분석 결과: ratio={target_ratio:.2f}, drag={drag_distance}px")
        else:
            # 분석 실패 시 랜덤 오프셋
            target_ratio = random.uniform(0.3, 0.7)
            drag_distance = int(usable_width * target_ratio)
            logger.info(f"이미지 분석 실패, 랜덤 시도: ratio={target_ratio:.2f}, drag={drag_distance}px")

        # 3. 인간처럼 드래그
        await self._human_drag(slider_button, drag_distance)
        return True

    async def _analyze_puzzle_images(self) -> Optional[float]:
        """
        퍼즐 배경 이미지에서 갭 위치를 분석합니다.

        Returns:
            갭 위치의 비율 (0.0~1.0) 또는 None (분석 실패)
        """
        try:
            page = self.page

            # 배경 이미지와 퍼즐 조각 가져오기
            bg_img_el = await page.query_selector(self.BG_IMAGE_SEL)
            piece_img_el = await page.query_selector(self.PIECE_IMAGE_SEL)

            if not bg_img_el or not piece_img_el:
                logger.warning("퍼즐 이미지 요소를 찾을 수 없음")
                return None

            # 이미지 데이터 추출 (base64 src에서)
            bg_src = await bg_img_el.get_attribute("src")
            piece_src = await piece_img_el.get_attribute("src")

            if not bg_src or not piece_src:
                return None

            bg_image = self._decode_data_uri(bg_src)
            piece_image = self._decode_data_uri(piece_src)

            if not bg_image or not piece_image:
                # base64가 아닌 경우 스크린샷으로 대체
                return await self._analyze_from_screenshot()

            # 배경 이미지에서 갭 위치 탐지
            return self._find_gap_position(bg_image, piece_image)

        except Exception as e:
            logger.warning(f"퍼즐 이미지 분석 오류: {e}")
            return None

    @staticmethod
    def _decode_data_uri(data_uri: str) -> Optional[Image.Image]:
        """data:image URI를 PIL Image로 디코딩"""
        import base64

        if not data_uri.startswith("data:image"):
            return None

        try:
            # data:image/webp;base64,... 형식
            _header, data = data_uri.split(",", 1)
            image_bytes = base64.b64decode(data)
            return Image.open(io.BytesIO(image_bytes))
        except Exception:
            return None

    @staticmethod
    def _find_gap_position(
        bg_image: Image.Image, _piece_image: Image.Image
    ) -> Optional[float]:
        """
        배경 이미지에서 퍼즐 갭의 x 위치를 비율로 반환합니다.

        배경 이미지의 에지를 분석하여 퍼즐 조각이 빠진 갭의 위치를 찾습니다.
        """
        try:
            # 그레이스케일 변환
            bg_gray = bg_image.convert("L")

            # 에지 디텍션
            edges = bg_gray.filter(ImageFilter.FIND_EDGES)

            width, height = edges.size

            # 원형 이미지이므로 중앙 영역만 분석 (상하 20% 제외)
            y_start = int(height * 0.2)
            y_end = int(height * 0.8)

            # 열별 에지 강도 합산
            col_edge_sums = []
            for x in range(width):
                col_sum = 0
                for y in range(y_start, y_end):
                    col_sum += edges.getpixel((x, y))
                col_edge_sums.append(col_sum)

            # 퍼즐 조각 크기 비율 (퍼즐은 보통 배경의 60% 크기)
            piece_width_ratio = 0.6

            # 갭은 보통 배경의 왼쪽 20% 이후에 위치 (슬라이더 시작점 제외)
            search_start = int(width * 0.2)
            search_end = int(width * 0.85)

            # 검색 범위 내에서 에지 강도가 가장 높은 구간 찾기
            # (갭의 경계에서 에지가 강하게 나타남)
            window_size = int(width * piece_width_ratio * 0.15)  # 윈도우 크기
            max_sum = 0
            max_pos = search_start

            for x in range(search_start, search_end - window_size):
                window_sum = sum(col_edge_sums[x : x + window_size])
                if window_sum > max_sum:
                    max_sum = window_sum
                    max_pos = x

            # 갭 중심의 x 비율 반환
            gap_center = max_pos + window_size // 2
            ratio = gap_center / width

            logger.info(f"갭 탐지: pos={gap_center}/{width}, ratio={ratio:.3f}")
            return ratio

        except Exception as e:
            logger.warning(f"갭 위치 분석 실패: {e}")
            return None

    async def _analyze_from_screenshot(self) -> Optional[float]:
        """스크린샷 기반 갭 위치 분석 (이미지 직접 접근 실패 시 폴백)"""
        try:
            container = await self.page.query_selector(self.CONTAINER_SEL)
            if not container:
                return None

            screenshot_bytes = await container.screenshot()
            screenshot = Image.open(io.BytesIO(screenshot_bytes))

            # 컨테이너 전체에서 퍼즐 영역 추출
            # (DOM 분석 결과: 퍼즐은 컨테이너 상단 중앙에 위치)
            w, h = screenshot.size
            # 퍼즐 영역: 대략 상단 60%
            puzzle_area = screenshot.crop((0, 0, w, int(h * 0.7)))

            gray = puzzle_area.convert("L")
            edges = gray.filter(ImageFilter.FIND_EDGES)

            pw, ph = edges.size
            y_start = int(ph * 0.15)
            y_end = int(ph * 0.85)

            col_sums = []
            for x in range(pw):
                s = sum(edges.getpixel((x, y)) for y in range(y_start, y_end))
                col_sums.append(s)

            search_start = int(pw * 0.25)
            search_end = int(pw * 0.75)
            window = max(3, int(pw * 0.05))

            max_sum = 0
            max_pos = search_start

            for x in range(search_start, search_end - window):
                ws = sum(col_sums[x : x + window])
                if ws > max_sum:
                    max_sum = ws
                    max_pos = x

            ratio = (max_pos + window // 2) / pw
            logger.info(f"스크린샷 기반 갭 탐지: ratio={ratio:.3f}")
            return ratio

        except Exception as e:
            logger.warning(f"스크린샷 기반 분석 실패: {e}")
            return None

    async def _human_drag(self, slider_element, distance: int):
        """인간처럼 슬라이더를 드래그합니다."""
        page = self.page
        box = await slider_element.bounding_box()
        if not box:
            return

        start_x = box["x"] + box["width"] / 2
        start_y = box["y"] + box["height"] / 2

        # 인간형 드래그 경로 생성
        path = self._generate_human_path(distance)

        # 마우스 다운
        await page.mouse.move(start_x, start_y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await page.mouse.down()
        await asyncio.sleep(random.uniform(0.05, 0.15))

        # 경로를 따라 이동
        for dx, dy, delay in path:
            await page.mouse.move(start_x + dx, start_y + dy)
            await asyncio.sleep(delay)

        # 마우스 업
        await asyncio.sleep(random.uniform(0.05, 0.2))
        await page.mouse.up()

    @staticmethod
    def _generate_human_path(distance: int) -> list[tuple[float, float, float]]:
        """
        인간처럼 보이는 드래그 경로를 생성합니다.

        Returns:
            [(dx, dy, delay), ...] 형태의 경로
        """
        path = []
        steps = random.randint(25, 40)
        overshoot = random.uniform(1.02, 1.08)  # 2~8% 오버슈트

        for i in range(steps):
            t = i / steps
            # 이징: 시작 느리게, 중간 빠르게, 끝 느리게 (ease-in-out)
            eased_t = 0.5 - 0.5 * math.cos(math.pi * t)

            # 오버슈트 적용 (마지막 20%에서 돌아옴)
            if t < 0.8:
                progress = eased_t * overshoot
            else:
                # 오버슈트에서 최종 위치로 복귀
                return_t = (t - 0.8) / 0.2
                progress = overshoot * eased_t - (overshoot - 1.0) * return_t * eased_t

            dx = distance * min(progress, overshoot)

            # y축 약간의 떨림
            dy = random.gauss(0, 1.5)

            # 속도에 따른 딜레이
            if t < 0.2:
                delay = random.uniform(0.015, 0.035)  # 시작: 느리게
            elif t < 0.7:
                delay = random.uniform(0.005, 0.015)  # 중간: 빠르게
            else:
                delay = random.uniform(0.01, 0.03)  # 끝: 느리게

            path.append((dx, dy, delay))

        # 최종 위치 보정
        path.append((float(distance), 0.0, random.uniform(0.02, 0.05)))
        return path

    async def _refresh(self):
        """캡차 리프레시"""
        try:
            refresh_btn = await self.page.query_selector(self.REFRESH_SEL)
            if refresh_btn:
                await refresh_btn.click()
                await asyncio.sleep(1.5)
                logger.info("캡차 리프레시 완료")
        except Exception:
            pass
