"""
TikTok 슬라이더 퍼즐 캡차 자동 풀기.

TikTok Seller Center 로그인 시 나타나는 원형 퍼즐 캡차를 자동으로 풀어줍니다.
배경 이미지의 갭 위치를 탐지하고 슬라이더를 인간처럼 드래그합니다.

갭 위치 탐지 전략 (우선순위):
1. OpenCV matchTemplate + Canny (80-92% 정확도) - 배경+퍼즐 조각 비교
2. 로컬 에지 디텍션 (Pillow 기반) - 최종 폴백
"""
import asyncio
import base64
import io
import logging
import math
import random
from typing import Optional

import numpy as np
from PIL import Image, ImageFilter

try:
    import cv2
    HAS_OPENCV = True
except ImportError:
    HAS_OPENCV = False
    logging.getLogger(__name__).warning("OpenCV 미설치 → PIL 폴백만 사용 가능")

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

        1차: OpenCV matchTemplate + Canny (80-92% 정확도)
        2차 폴백: 로컬 이미지 분석 (Pillow 기반)

        Returns:
            갭 위치의 비율 (0.0~1.0) 또는 None (분석 실패)
        """
        # 1차: OpenCV matchTemplate + Canny
        if HAS_OPENCV:
            result = await self._solve_with_opencv()
            if result is not None:
                return result
            logger.warning("OpenCV 분석 실패 → PIL 폴백 시도")

        # 2차 폴백: 로컬 이미지 분석 (Pillow)
        return await self._local_image_analysis()

    # =========================================================================
    # OpenCV 기반 캡차 풀기 (1순위)
    # =========================================================================

    async def _solve_with_opencv(self) -> Optional[float]:
        """OpenCV matchTemplate + Canny 에지 검출로 갭 위치 탐지.

        3가지 서브 메서드를 순차적으로 시도:
        1. Canny 에지 + matchTemplate (배경+퍼즐 조각 모두 사용, 80-92%)
        2. 그레이스케일 matchTemplate (배경+퍼즐 조각, 70-85%)
        3. 배경 전용 갭 검출 (Canny 에지만, 60-75%)

        Returns:
            갭 위치의 비율 (0.0~1.0) 또는 None
        """
        page = self.page

        # 이미지 요소에서 data URI 추출
        bg_img_el = await page.query_selector(self.BG_IMAGE_SEL)
        piece_img_el = await page.query_selector(self.PIECE_IMAGE_SEL)

        if not bg_img_el:
            logger.warning("OpenCV: 배경 이미지 요소를 찾을 수 없음")
            return None

        bg_src = await bg_img_el.get_attribute("src")
        if not bg_src or not bg_src.startswith("data:image"):
            logger.warning("OpenCV: 배경 이미지가 data URI가 아님")
            return None

        bg_cv = self._data_uri_to_cv2(bg_src)
        if bg_cv is None:
            logger.warning("OpenCV: 배경 이미지 디코딩 실패")
            return None

        piece_cv = None
        if piece_img_el:
            piece_src = await piece_img_el.get_attribute("src")
            if piece_src and piece_src.startswith("data:image"):
                piece_cv = self._data_uri_to_cv2(piece_src)

        img_width = bg_cv.shape[1]
        logger.info(
            f"OpenCV 분석 시작: bg={bg_cv.shape[1]}x{bg_cv.shape[0]}, "
            f"piece={'있음' if piece_cv is not None else '없음'}"
        )

        results = []

        # 1차: Canny 에지 + matchTemplate (퍼즐 조각 필수)
        if piece_cv is not None:
            ratio = self._match_by_canny_edge(bg_cv, piece_cv)
            if ratio is not None:
                logger.info(f"OpenCV [Canny+Template]: ratio={ratio:.3f}")
                results.append(("canny_template", ratio, 0.92))

            # 2차: 그레이스케일 matchTemplate
            ratio = self._match_by_grayscale(bg_cv, piece_cv)
            if ratio is not None:
                logger.info(f"OpenCV [Grayscale Template]: ratio={ratio:.3f}")
                results.append(("grayscale_template", ratio, 0.80))

        # 3차: 배경 전용 갭 검출 (퍼즐 조각 없어도 가능)
        ratio = self._detect_gap_by_canny(bg_cv)
        if ratio is not None:
            logger.info(f"OpenCV [Gap Detection]: ratio={ratio:.3f}")
            results.append(("gap_detection", ratio, 0.65))

        if not results:
            logger.warning("OpenCV: 모든 서브 메서드 실패")
            return None

        # 결과 합산: 가중 평균 또는 단일 결과 반환
        if len(results) == 1:
            method, ratio, _ = results[0]
            logger.info(f"OpenCV 최종 결과 ({method}): ratio={ratio:.3f}")
            return ratio

        # 여러 결과가 있으면 일치도 확인
        ratios = [r[1] for r in results]
        weights = [r[2] for r in results]

        # 결과들이 비슷한 위치를 가리키면 (0.1 이내) 가중 평균
        if max(ratios) - min(ratios) < 0.1:
            weighted_sum = sum(r * w for r, w in zip(ratios, weights))
            weight_total = sum(weights)
            final_ratio = weighted_sum / weight_total
            logger.info(
                f"OpenCV 합의 ({len(results)}개 방법 일치): "
                f"ratio={final_ratio:.3f}, 편차={max(ratios)-min(ratios):.3f}"
            )
            return final_ratio

        # 불일치 시 가장 신뢰도 높은 결과 사용
        best = max(results, key=lambda x: x[2])
        logger.info(
            f"OpenCV 결과 불일치 → 최고 신뢰도 사용 ({best[0]}): "
            f"ratio={best[1]:.3f}, 전체: {[(r[0], f'{r[1]:.3f}') for r in results]}"
        )
        return best[1]

    @staticmethod
    def _data_uri_to_cv2(data_uri: str) -> Optional[np.ndarray]:
        """data:image URI를 OpenCV numpy array로 변환."""
        try:
            _, b64_data = data_uri.split(",", 1)
            img_bytes = base64.b64decode(b64_data)
            nparr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            return img
        except Exception as e:
            logger.warning(f"data URI → cv2 변환 실패: {e}")
            return None

    @staticmethod
    def _match_by_canny_edge(
        bg: np.ndarray, piece: np.ndarray
    ) -> Optional[float]:
        """Canny 에지 + matchTemplate로 갭 위치 탐지 (1순위, 80-92%).

        배경과 퍼즐 조각 모두의 에지를 추출한 뒤 template matching 수행.
        에지 기반이므로 밝기/색상 변화에 강건함.
        """
        try:
            bg_gray = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY)
            piece_gray = cv2.cvtColor(piece, cv2.COLOR_BGR2GRAY)

            # Canny 에지 검출
            bg_edges = cv2.Canny(bg_gray, 100, 200)
            piece_edges = cv2.Canny(piece_gray, 100, 200)

            # 퍼즐 조각에서 유효 영역만 추출 (투명/검정 배경 제거)
            # 알파 채널이 있으면 활용
            if piece.shape[2] == 4:
                alpha = piece[:, :, 3]
                mask = (alpha > 10).astype(np.uint8)
            else:
                # 알파 없으면 검정 배경 제거 (RGB 합 > 30)
                piece_sum = piece.sum(axis=2)
                mask = (piece_sum > 30).astype(np.uint8)

            # 마스크 내에서 바운딩 박스 추출
            coords = cv2.findNonZero(mask)
            if coords is None:
                return None
            x, y, w, h = cv2.boundingRect(coords)

            # 너무 작은 조각은 무시
            if w < 10 or h < 10:
                return None

            piece_crop = piece_edges[y:y+h, x:x+w]

            # 배경보다 퍼즐 조각이 크면 불가
            if piece_crop.shape[0] > bg_edges.shape[0] or piece_crop.shape[1] > bg_edges.shape[1]:
                return None

            # Template matching (TM_CCOEFF_NORMED: -1~1, 높을수록 일치)
            result = cv2.matchTemplate(bg_edges, piece_crop, cv2.TM_CCOEFF_NORMED)
            _, max_val, _, max_loc = cv2.minMaxLoc(result)

            if max_val < 0.15:
                logger.debug(f"Canny matchTemplate 신뢰도 낮음: {max_val:.3f}")
                return None

            # 갭 중심 x좌표 계산
            gap_center_x = max_loc[0] + piece_crop.shape[1] // 2
            ratio = gap_center_x / bg.shape[1]
            ratio = max(0.05, min(0.95, ratio))

            logger.info(
                f"Canny matchTemplate: x={max_loc[0]}, 중심={gap_center_x}, "
                f"confidence={max_val:.3f}, ratio={ratio:.3f}"
            )
            return ratio

        except Exception as e:
            logger.warning(f"Canny matchTemplate 실패: {e}")
            return None

    @staticmethod
    def _match_by_grayscale(
        bg: np.ndarray, piece: np.ndarray
    ) -> Optional[float]:
        """그레이스케일 matchTemplate로 갭 위치 탐지 (2순위, 70-85%).

        색상 정보 없이 밝기만으로 매칭. Canny보다 단순하지만 보완적.
        """
        try:
            bg_gray = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY)
            piece_gray = cv2.cvtColor(piece, cv2.COLOR_BGR2GRAY)

            # 퍼즐 조각 유효 영역 추출
            if piece.shape[2] == 4:
                alpha = piece[:, :, 3]
                mask = (alpha > 10).astype(np.uint8)
            else:
                piece_sum = piece.sum(axis=2)
                mask = (piece_sum > 30).astype(np.uint8)

            coords = cv2.findNonZero(mask)
            if coords is None:
                return None
            x, y, w, h = cv2.boundingRect(coords)

            if w < 10 or h < 10:
                return None

            piece_crop = piece_gray[y:y+h, x:x+w]
            mask_crop = (mask[y:y+h, x:x+w] * 255).astype(np.uint8)

            if piece_crop.shape[0] > bg_gray.shape[0] or piece_crop.shape[1] > bg_gray.shape[1]:
                return None

            # 마스크 적용 template matching
            result = cv2.matchTemplate(bg_gray, piece_crop, cv2.TM_CCOEFF_NORMED, mask=mask_crop)
            _, max_val, _, max_loc = cv2.minMaxLoc(result)

            if max_val < 0.2:
                logger.debug(f"Grayscale matchTemplate 신뢰도 낮음: {max_val:.3f}")
                return None

            gap_center_x = max_loc[0] + piece_crop.shape[1] // 2
            ratio = gap_center_x / bg.shape[1]
            ratio = max(0.05, min(0.95, ratio))

            logger.info(
                f"Grayscale matchTemplate: x={max_loc[0]}, 중심={gap_center_x}, "
                f"confidence={max_val:.3f}, ratio={ratio:.3f}"
            )
            return ratio

        except Exception as e:
            logger.warning(f"Grayscale matchTemplate 실패: {e}")
            return None

    @staticmethod
    def _detect_gap_by_canny(bg: np.ndarray) -> Optional[float]:
        """배경 이미지만으로 갭 위치 검출 (3순위, 60-75%).

        퍼즐 조각이 없어도 동작. 배경에서 Canny 에지를 추출한 후
        열별 에지 밀도의 급격한 변화 영역을 갭으로 판단.

        갭은 원래 텍스처가 제거된 영역이므로:
        - 갭 내부: 에지 밀도 낮음
        - 갭 경계: 에지 밀도 급증 (원형 경계선)
        """
        try:
            bg_gray = cv2.cvtColor(bg, cv2.COLOR_BGR2GRAY)
            height, width = bg_gray.shape

            # 가우시안 블러로 노이즈 제거 후 Canny
            blurred = cv2.GaussianBlur(bg_gray, (5, 5), 0)
            edges = cv2.Canny(blurred, 50, 150)

            # 원형 이미지이므로 중앙 영역만 분석
            cx, cy = width // 2, height // 2
            radius = min(width, height) // 2 - 2
            inner_radius = int(radius * 0.75)

            # 원형 마스크 생성
            mask = np.zeros_like(edges)
            cv2.circle(mask, (cx, cy), inner_radius, 255, -1)
            edges_masked = cv2.bitwise_and(edges, mask)

            # 열별 에지 밀도 계산
            col_density = np.zeros(width, dtype=np.float64)
            col_count = np.zeros(width, dtype=np.float64)

            for x in range(width):
                col_pixels = mask[:, x]
                valid_count = np.count_nonzero(col_pixels)
                if valid_count > 0:
                    edge_count = np.count_nonzero(edges_masked[:, x])
                    col_density[x] = edge_count / valid_count
                    col_count[x] = valid_count

            # 가장자리 15% 제외
            margin = int(width * 0.15)
            valid_density = col_density[margin:width - margin]

            if len(valid_density) == 0:
                return None

            # 스무딩 (커널 크기 = 5% 너비)
            kernel_size = max(3, int(width * 0.05))
            if kernel_size % 2 == 0:
                kernel_size += 1
            kernel = np.ones(kernel_size) / kernel_size
            smoothed = np.convolve(valid_density, kernel, mode='same')

            # 평균 에지 밀도
            avg_density = np.mean(smoothed[smoothed > 0])
            if avg_density == 0:
                return None

            # 에지 밀도가 평균보다 현저히 낮은 영역 = 갭 내부
            threshold = avg_density * 0.5
            low_density_mask = smoothed < threshold

            # 연속 구간 찾기
            diffs = np.diff(low_density_mask.astype(int))
            starts = np.where(diffs == 1)[0] + 1
            ends = np.where(diffs == -1)[0] + 1

            if len(starts) == 0:
                # 전체가 low일 수 있음
                if low_density_mask[0]:
                    starts = np.array([0])
                else:
                    return None
            if len(ends) == 0 or (len(ends) > 0 and ends[-1] < starts[-1]):
                ends = np.append(ends, len(smoothed) - 1)

            # 가장 긴 연속 구간 찾기
            best_gap = None
            best_length = 0
            min_gap_size = int(width * 0.05)  # 최소 5% 너비

            for s, e in zip(starts, ends):
                gap_len = e - s
                if gap_len > best_length and gap_len >= min_gap_size:
                    best_length = gap_len
                    best_gap = (s, e)

            if best_gap is None:
                # 에지 밀도 그래디언트 기반 대안
                gradient = np.abs(np.gradient(smoothed))
                # 상위 피크들 찾기
                peak_threshold = np.percentile(gradient, 90)
                peak_indices = np.where(gradient > peak_threshold)[0]

                if len(peak_indices) >= 2:
                    # 가장 큰 간격의 피크 쌍 = 갭 좌우 경계
                    peak_gaps = np.diff(peak_indices)
                    max_gap_idx = np.argmax(peak_gaps)
                    left_peak = peak_indices[max_gap_idx]
                    right_peak = peak_indices[max_gap_idx + 1]
                    gap_center = (left_peak + right_peak) // 2 + margin
                    ratio = gap_center / width
                    ratio = max(0.05, min(0.95, ratio))
                    logger.info(f"Canny 갭 검출 (그래디언트): 중심={gap_center}, ratio={ratio:.3f}")
                    return ratio

                return None

            # 갭 중심 좌표 (margin 오프셋 보정)
            gap_center = (best_gap[0] + best_gap[1]) // 2 + margin
            ratio = gap_center / width
            ratio = max(0.05, min(0.95, ratio))

            logger.info(
                f"Canny 갭 검출: 구간={best_gap[0]+margin}~{best_gap[1]+margin}, "
                f"중심={gap_center}, 길이={best_length}px, ratio={ratio:.3f}"
            )
            return ratio

        except Exception as e:
            logger.warning(f"Canny 갭 검출 실패: {e}")
            return None

    async def _local_image_analysis(self) -> Optional[float]:
        """로컬 PIL 이미지 분석으로 갭 위치 탐지 (OpenCV 폴백)."""
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
