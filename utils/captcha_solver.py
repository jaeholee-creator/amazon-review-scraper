"""
TikTok 슬라이더 퍼즐 캡차 자동 풀기.

TikTok Seller Center 로그인 시 나타나는 원형 퍼즐 캡차를 자동으로 풀어줍니다.
배경 이미지의 갭 위치를 탐지하고 슬라이더를 인간처럼 드래그합니다.

갭 위치 탐지 전략 (우선순위):
1. Euler Stream API (무료 25건/일, 99.2% 정확도) - EULER_STREAM_API_KEY 설정 시
2. 로컬 에지 디텍션 (Pillow 기반) - 폴백
"""
import asyncio
import base64
import io
import logging
import math
import os
import random
from typing import Optional

import aiohttp
from PIL import Image, ImageFilter

logger = logging.getLogger(__name__)


class TikTokCaptchaSolver:
    """TikTok 슬라이더 퍼즐 캡차 자동 풀기"""

    MAX_RETRIES = 3  # rate limit 방지: 5 → 3
    EULER_API_URL = "https://api.eulerstream.com/captchas/puzzle"

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
        """슬라이더 캡차가 화면에 표시되어 있는지 확인."""
        # CSS 셀렉터 기반 확인
        slider = await self.page.query_selector(self.SLIDER_TRACK_SEL)
        if slider:
            return True
        bg_img = await self.page.query_selector(self.BG_IMAGE_SEL)
        if bg_img:
            return True
        # 텍스트 기반 확인 (셀렉터가 변경되었을 수 있으므로 폴백)
        try:
            body_text = await self.page.evaluate("() => document.body.innerText")
            if "Drag the slider" in body_text or "drag the slider" in body_text:
                return True
        except Exception:
            pass
        return False

    async def solve(self) -> bool:
        """캡차 풀기 (최대 MAX_RETRIES번 시도). 실패 시 오프셋 지터 적용."""
        self._jitter_offset = 0.0  # 재시도 시 적용할 오프셋

        for attempt in range(1, self.MAX_RETRIES + 1):
            if not await self.is_captcha_visible():
                logger.info("캡차가 없음 - 통과")
                return True

            logger.info(f"캡차 풀기 시도 {attempt}/{self.MAX_RETRIES}")

            try:
                success = await self._attempt_solve()

                if not success:
                    await asyncio.sleep(1)
                    if not await self.is_captcha_visible():
                        logger.info("캡차 풀기 성공! (요소 소멸 확인)")
                        return True
                    logger.info("캡차 풀기 실패 - 리프레시 후 재시도")
                    await self._refresh()
                    await asyncio.sleep(2)
                    # 재시도마다 오프셋 변경
                    self._jitter_offset = random.uniform(-0.05, 0.05) * attempt
                    continue

                # 드래그 후 서버 검증 대기
                for wait_i in range(6):
                    await asyncio.sleep(1.5)
                    if not await self.is_captcha_visible():
                        await asyncio.sleep(2)
                        if not await self.is_captcha_visible():
                            logger.info(f"캡차 풀기 성공! ({wait_i + 1}회차 확인)")
                            return True
                        logger.info("캡차가 잠깐 사라졌다가 재표시됨 - 재시도 필요")
                        break

                logger.info("드래그 후 캡차 여전히 표시 - 리프레시 후 재시도")
                await self._refresh()
                await asyncio.sleep(2)
                # 재시도마다 오프셋 변경
                self._jitter_offset = random.uniform(-0.05, 0.05) * attempt

            except Exception as e:
                logger.warning(f"캡차 풀기 시도 {attempt} 오류: {e}")
                await asyncio.sleep(1)
                if not await self.is_captcha_visible():
                    return True
                await self._refresh()
                await asyncio.sleep(2)
                self._jitter_offset = random.uniform(-0.05, 0.05) * attempt

        logger.error(f"캡차 풀기 최종 실패 ({self.MAX_RETRIES}번 시도)")
        return False

    async def _attempt_solve(self) -> bool:
        """단일 캡차 풀기 시도"""
        page = self.page

        # 1. 슬라이더 버튼 및 트랙 찾기
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

        # 2. 퍼즐 조각/배경 이미지 크기 측정 (디버깅 로그)
        bg_img_el = await page.query_selector(self.BG_IMAGE_SEL)
        piece_img_el = await page.query_selector(self.PIECE_IMAGE_SEL)
        if bg_img_el and piece_img_el:
            bg_box = await bg_img_el.bounding_box()
            piece_box = await piece_img_el.bounding_box()
            if bg_box and piece_box:
                logger.info(
                    f"퍼즐 렌더링 크기: bg={bg_box['width']:.0f}x{bg_box['height']:.0f}, "
                    f"piece={piece_box['width']:.0f}x{piece_box['height']:.0f}"
                )

        # 3. 퍼즐 이미지 분석하여 타겟 위치 계산
        target_ratio = await self._analyze_puzzle_images()

        # 재시도 시 지터 오프셋 적용
        jitter = getattr(self, '_jitter_offset', 0.0)

        if target_ratio is not None:
            adjusted_ratio = max(0.05, min(0.95, target_ratio + jitter))
            drag_distance = int(usable_width * adjusted_ratio)
            logger.info(
                f"이미지 분석 결과: ratio={target_ratio:.2f}, "
                f"jitter={jitter:+.2f}, adjusted={adjusted_ratio:.2f}, drag={drag_distance}px"
            )
        else:
            target_ratio = random.uniform(0.2, 0.7)
            drag_distance = int(usable_width * target_ratio)
            logger.info(f"이미지 분석 실패, 랜덤 시도: ratio={target_ratio:.2f}, drag={drag_distance}px")

        # 4. 인간처럼 드래그
        await self._human_drag(slider_button, drag_distance)
        return True

    async def _analyze_puzzle_images(self) -> Optional[float]:
        """
        퍼즐 배경 이미지에서 갭 위치를 분석합니다.

        1차: EulerStream API (99.2% 정확도, 30-40ms)
        2차 폴백: 로컬 이미지 분석 (에지 + 밝기 하이브리드)

        Returns:
            갭 위치의 비율 (0.0~1.0) 또는 None (분석 실패)
        """
        # 1차: EulerStream API
        api_key = os.environ.get("EULER_STREAM_API_KEY", "")
        if api_key:
            result = await self._solve_with_euler_api(api_key)
            if result is not None:
                return result
            logger.warning("EulerStream API 실패 → 로컬 이미지 분석으로 폴백")

        # 2차 폴백: 로컬 이미지 분석
        return await self._local_image_analysis()

    async def _solve_with_euler_api(self, api_key: str) -> Optional[float]:
        """EulerStream API로 퍼즐 x좌표를 획득하여 ratio로 변환."""
        try:
            page = self.page
            bg_img_el = await page.query_selector(self.BG_IMAGE_SEL)
            if not bg_img_el:
                logger.warning("EulerStream: 배경 이미지 요소를 찾을 수 없음")
                return None

            bg_src = await bg_img_el.get_attribute("src")
            if not bg_src or not bg_src.startswith("data:image"):
                logger.warning("EulerStream: 배경 이미지가 data URI가 아님")
                return None

            # data:image/webp;base64,... 에서 base64 데이터 추출
            _header, b64_data = bg_src.split(",", 1)

            # 이미지 너비 측정 (ratio 계산용)
            image_bytes = base64.b64decode(b64_data)
            bg_image = Image.open(io.BytesIO(image_bytes))
            img_width = bg_image.width

            # EulerStream API 호출
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.EULER_API_URL,
                    headers={
                        "x-api-key": api_key,
                        "Content-Type": "application/json",
                    },
                    json={"puzzle": b64_data},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    if resp.status != 200:
                        body = await resp.text()
                        logger.warning(f"EulerStream API 오류: {resp.status} - {body}")
                        return None

                    data = await resp.json()

            if data.get("code") != 200:
                logger.warning(f"EulerStream API 응답 코드 이상: {data}")
                return None

            x_pos = data.get("response", {}).get("x")
            time_ms = data.get("response", {}).get("time_ms", 0)

            if x_pos is None:
                logger.warning(f"EulerStream API: x좌표 없음 - {data}")
                return None

            ratio = x_pos / img_width
            ratio = max(0.05, min(0.95, ratio))
            logger.info(
                f"EulerStream API 성공: x={x_pos}, width={img_width}, "
                f"ratio={ratio:.3f}, 응답={time_ms}ms"
            )
            return ratio

        except Exception as e:
            logger.warning(f"EulerStream API 호출 실패: {e}")
            return None

    async def _local_image_analysis(self) -> Optional[float]:
        """로컬 이미지 분석으로 갭 위치 탐지 (EulerStream 폴백)."""
        try:
            page = self.page

            bg_img_el = await page.query_selector(self.BG_IMAGE_SEL)
            piece_img_el = await page.query_selector(self.PIECE_IMAGE_SEL)

            if not bg_img_el or not piece_img_el:
                logger.warning("퍼즐 이미지 요소를 찾을 수 없음")
                return None

            bg_src = await bg_img_el.get_attribute("src")
            piece_src = await piece_img_el.get_attribute("src")

            if not bg_src or not piece_src:
                return None

            bg_image = self._decode_data_uri(bg_src)
            piece_image = self._decode_data_uri(piece_src)

            if not bg_image or not piece_image:
                return await self._analyze_from_screenshot()

            edge_ratio = self._find_gap_by_edge_energy(bg_image)
            brightness_ratio = self._find_gap_position(bg_image, piece_image)

            if edge_ratio is not None and brightness_ratio is not None:
                if abs(edge_ratio - brightness_ratio) < 0.15:
                    final = (edge_ratio + brightness_ratio) / 2
                    logger.info(
                        f"에지({edge_ratio:.3f}) + 밝기({brightness_ratio:.3f}) 합의 → {final:.3f}"
                    )
                    return final
                logger.info(
                    f"에지({edge_ratio:.3f}) vs 밝기({brightness_ratio:.3f}) 불일치 → 에지 우선"
                )
                return edge_ratio

            return edge_ratio or brightness_ratio

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
        bg_image: Image.Image, piece_image: Image.Image
    ) -> Optional[float]:
        """
        원형 퍼즐 배경 이미지에서 갭(빠진 조각)의 x 위치를 비율로 반환합니다.

        TikTok 캡차는 원형 퍼즐이므로:
        1. 원형 테두리의 에지를 제외하고 내부만 분석
        2. 갭은 배경색(밝은 회색)이 보이는 영역 → 주변보다 밝기가 다름
        3. 열별 밝기 이상치를 탐지하여 갭 위치 결정
        """
        try:
            bg_gray = bg_image.convert("L")
            width, height = bg_gray.size
            cx, cy = width // 2, height // 2
            radius = min(width, height) // 2 - 2

            # 1. 원형 마스크 내부에서 열별 평균 밝기 계산
            col_brightness = []
            col_pixel_count = []
            inner_radius = radius * 0.80  # 테두리 20% 제외

            for x in range(width):
                brightness_sum = 0
                pixel_count = 0
                for y in range(height):
                    dist = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
                    if dist < inner_radius:
                        brightness_sum += bg_gray.getpixel((x, y))
                        pixel_count += 1
                if pixel_count > 0:
                    col_brightness.append(brightness_sum / pixel_count)
                else:
                    col_brightness.append(0)
                col_pixel_count.append(pixel_count)

            # 유효한 열만 선택 (충분한 픽셀이 있는 열)
            min_pixels = max(col_pixel_count) * 0.3
            valid_cols = [i for i in range(width) if col_pixel_count[i] > min_pixels]
            if not valid_cols:
                return None

            # 2. 열별 밝기의 로컬 이상치 탐지
            # 갭은 주변 열과 밝기가 크게 다른 영역
            window = max(3, int(width * 0.06))
            search_start = valid_cols[0] + window
            search_end = valid_cols[-1] - window

            max_anomaly = 0
            max_pos = search_start
            anomaly_scores = []

            for x in range(search_start, search_end):
                if col_pixel_count[x] < min_pixels:
                    anomaly_scores.append(0)
                    continue

                # 현재 열의 밝기
                current = col_brightness[x]

                # 주변 열의 평균 밝기 (좌우 window 범위, 현재 열 제외)
                left_vals = [col_brightness[i] for i in range(max(0, x - window * 2), x - window // 2)
                             if col_pixel_count[i] > min_pixels]
                right_vals = [col_brightness[i] for i in range(x + window // 2, min(width, x + window * 2))
                              if col_pixel_count[i] > min_pixels]

                if not left_vals or not right_vals:
                    anomaly_scores.append(0)
                    continue

                surround_avg = (sum(left_vals) + sum(right_vals)) / (len(left_vals) + len(right_vals))
                anomaly = abs(current - surround_avg)
                anomaly_scores.append(anomaly)

                if anomaly > max_anomaly:
                    max_anomaly = anomaly
                    max_pos = x

            # 3. 이상치가 충분히 큰 연속 영역의 중심을 갭 위치로 결정
            if max_anomaly < 3:
                logger.warning(f"밝기 이상치가 너무 작음: {max_anomaly:.1f}")
                return None

            # 임계값: 최대 이상치의 40%
            threshold = max_anomaly * 0.4
            gap_cols = []
            for i, score in enumerate(anomaly_scores):
                if score >= threshold:
                    gap_cols.append(search_start + i)

            if not gap_cols:
                return None

            # 가장 큰 연속 구간 찾기
            groups = []
            current_group = [gap_cols[0]]
            for i in range(1, len(gap_cols)):
                if gap_cols[i] - gap_cols[i - 1] <= 3:  # 3px 이내면 연속
                    current_group.append(gap_cols[i])
                else:
                    groups.append(current_group)
                    current_group = [gap_cols[i]]
            groups.append(current_group)

            # 가장 긴 연속 구간의 중심
            longest_group = max(groups, key=len)
            gap_center = (longest_group[0] + longest_group[-1]) // 2
            ratio = gap_center / width

            logger.info(
                f"갭 탐지 (밝기 이상치): pos={gap_center}/{width}, ratio={ratio:.3f}, "
                f"anomaly={max_anomaly:.1f}, 구간길이={len(longest_group)}"
            )
            return ratio

        except Exception as e:
            logger.warning(f"갭 위치 분석 실패: {e}")
            return None

    @staticmethod
    def _find_gap_by_edge_energy(bg_image: Image.Image) -> Optional[float]:
        """
        에지 검출 기반 갭 위치 탐지.

        배경 이미지에 Laplacian 에지 필터를 적용한 후,
        열별 에지 에너지(밀도)를 계산하여 갭 위치를 찾습니다.

        갭은 원래 이미지의 텍스처가 없는 빈 공간이므로
        주변보다 에지 에너지가 다릅니다 (경계는 높고, 내부는 낮음).
        """
        try:
            bg_gray = bg_image.convert("L")
            width, height = bg_gray.size
            cx, cy = width // 2, height // 2
            radius = min(width, height) // 2 - 2
            inner_radius = radius * 0.75

            # 에지 검출 (Laplacian → 더 강한 에지 감지)
            edges = bg_gray.filter(ImageFilter.FIND_EDGES)

            # 원형 마스크 내에서 열별 에지 에너지 계산
            col_energy = []
            for x in range(width):
                energy_sum = 0
                pixel_count = 0
                for y in range(height):
                    dist = ((x - cx) ** 2 + (y - cy) ** 2) ** 0.5
                    if dist < inner_radius:
                        energy_sum += edges.getpixel((x, y))
                        pixel_count += 1
                if pixel_count > 0:
                    col_energy.append(energy_sum / pixel_count)
                else:
                    col_energy.append(0)

            # 유효 범위: 가장자리 15% 제외 (원의 경계 효과 방지)
            margin = int(width * 0.15)
            valid_range = col_energy[margin:width - margin]
            if not valid_range:
                return None

            # 이동 평균으로 스무딩 (노이즈 제거)
            window = max(3, int(width * 0.04))
            smoothed = []
            for i in range(len(valid_range)):
                start = max(0, i - window)
                end = min(len(valid_range), i + window + 1)
                smoothed.append(sum(valid_range[start:end]) / (end - start))

            # 전체 평균과의 편차가 가장 큰 영역 = 갭 경계
            avg_energy = sum(smoothed) / len(smoothed)

            # 에지 에너지가 평균보다 높은 영역 찾기 (갭의 원형 경계)
            # 또는 낮은 영역 (갭 내부)
            # 두 가지 방법을 모두 시도하고 더 명확한 결과 사용

            # 방법 A: 에지 에너지 급증 구간 (갭 경계의 원형 에지)
            gradient = []
            for i in range(1, len(smoothed)):
                gradient.append(abs(smoothed[i] - smoothed[i - 1]))

            # 그래디언트의 피크 찾기 (갭 좌/우 경계)
            peak_threshold = max(gradient) * 0.4 if gradient else 0
            peaks = []
            for i, g in enumerate(gradient):
                if g >= peak_threshold:
                    peaks.append(margin + i)

            if len(peaks) >= 2:
                # 인접한 피크들을 그룹핑
                groups = []
                current_group = [peaks[0]]
                for i in range(1, len(peaks)):
                    if peaks[i] - peaks[i - 1] <= 5:
                        current_group.append(peaks[i])
                    else:
                        groups.append(current_group)
                        current_group = [peaks[i]]
                groups.append(current_group)

                # 가장 강한 두 그룹의 중심 = 갭의 좌우 경계
                groups.sort(key=lambda g: sum(gradient[p - margin] for p in g if 0 <= p - margin < len(gradient)), reverse=True)
                if len(groups) >= 2:
                    left_edge = (groups[0][0] + groups[0][-1]) // 2
                    right_edge = (groups[1][0] + groups[1][-1]) // 2
                    if left_edge > right_edge:
                        left_edge, right_edge = right_edge, left_edge
                    gap_center = (left_edge + right_edge) // 2
                    ratio = gap_center / width
                    logger.info(
                        f"갭 탐지 (에지 에너지): 좌={left_edge}, 우={right_edge}, "
                        f"중심={gap_center}/{width}, ratio={ratio:.3f}"
                    )
                    return ratio

            # 방법 B: 에지 에너지가 전체 평균보다 확연히 다른 영역
            anomaly_threshold = avg_energy * 0.6
            low_energy_cols = []
            for i, e in enumerate(smoothed):
                if e < anomaly_threshold:
                    low_energy_cols.append(margin + i)

            if low_energy_cols:
                # 가장 큰 연속 구간
                groups = []
                current_group = [low_energy_cols[0]]
                for i in range(1, len(low_energy_cols)):
                    if low_energy_cols[i] - low_energy_cols[i - 1] <= 3:
                        current_group.append(low_energy_cols[i])
                    else:
                        groups.append(current_group)
                        current_group = [low_energy_cols[i]]
                groups.append(current_group)

                longest = max(groups, key=len)
                if len(longest) >= 5:  # 최소 5px 이상
                    gap_center = (longest[0] + longest[-1]) // 2
                    ratio = gap_center / width
                    logger.info(
                        f"갭 탐지 (낮은 에지 에너지): 중심={gap_center}/{width}, "
                        f"ratio={ratio:.3f}, 구간={len(longest)}px"
                    )
                    return ratio

            logger.warning("에지 에너지 기반 갭 탐지 실패")
            return None

        except Exception as e:
            logger.warning(f"에지 에너지 분석 실패: {e}")
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
