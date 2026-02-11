const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const { GoogleSheetsUploader } = require('./google-sheets');

// ============================================================
// Shopee Review Crawler
// 저장된 세션 state를 사용하여 브라우저 내부에서 API 호출
// ============================================================

const CONFIG = {
  reviewUrl: 'https://seller.shopee.kr/portal/settings/shop/rating?cnsc_shop_id=951591050',
  authStatePath: path.join(__dirname, 'auth-state.json'),
  loginUrl: 'https://seller.shopee.kr/account/signin?next=%2F%3Fcnsc_shop_id%3D951591050',
  credentials: {
    username: process.env.SHOPEE_USERNAME || '',
    password: process.env.SHOPEE_PASSWORD || '',
  },
  slack: {
    botToken: process.env.SLACK_BOT_TOKEN || '',
    channelId: process.env.SLACK_CHANNEL_ID || '',
  },
  shops: [
    { region: 'sg', shopId: 951591050, name: 'Singapore' },
    { region: 'tw', shopId: 952094070, name: 'Taiwan' },
    { region: 'ph', shopId: 952094055, name: 'Philippines' },
  ],
  pageSize: 20,
  outputDir: path.join(__dirname, 'output'),
  requestDelay: 300,
};

// ============================================================
// Slack OTP Reader
// ============================================================
const getOtpFromSlack = async (afterTimestamp = null) => {
  const maxRetries = 30;
  const retryInterval = 2000;

  for (let i = 0; i < maxRetries; i++) {
    try {
      const resp = await fetch(
        `https://slack.com/api/conversations.history?channel=${CONFIG.slack.channelId}&limit=3`,
        { headers: { Authorization: `Bearer ${CONFIG.slack.botToken}` } }
      );
      const data = await resp.json();

      if (data.ok && data.messages?.length > 0) {
        for (const msg of data.messages) {
          if (afterTimestamp && parseFloat(msg.ts) <= afterTimestamp) continue;
          const match = msg.text?.match(/use OTP (\d{6})/);
          if (match) {
            console.log(`[OTP] Slack에서 OTP 발견: ${match[1]}`);
            return match[1];
          }
        }
      }
    } catch (err) {
      console.error(`[OTP] Slack API 오류:`, err.message);
    }
    console.log(`[OTP] OTP 대기 중... (${i + 1}/${maxRetries})`);
    await new Promise((r) => setTimeout(r, retryInterval));
  }
  throw new Error('OTP를 60초 내에 가져오지 못했습니다.');
};

// ============================================================
// Login (fallback if auth state is invalid)
// ============================================================
const login = async (page) => {
  console.log('[Login] 로그인 시도...');
  await page.goto(CONFIG.loginUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForTimeout(3000);

  const isLoggedIn = await page.evaluate(() => !window.location.href.includes('/account/signin'));
  if (isLoggedIn) {
    console.log('[Login] 이미 로그인됨');
    return;
  }

  let latestTs = null;
  try {
    const resp = await fetch(
      `https://slack.com/api/conversations.history?channel=${CONFIG.slack.channelId}&limit=1`,
      { headers: { Authorization: `Bearer ${CONFIG.slack.botToken}` } }
    );
    const data = await resp.json();
    if (data.ok && data.messages?.length > 0) latestTs = parseFloat(data.messages[0].ts);
  } catch (e) { /* ignore */ }

  await page.fill('input[placeholder="Email/Phone/Username"]', CONFIG.credentials.username);
  await page.fill('input[placeholder="Password"]', CONFIG.credentials.password);
  await page.click('button:has-text("Log In")');

  await page.waitForSelector('text=Verify Phone Number', { timeout: 15000 });
  const otp = await getOtpFromSlack(latestTs);

  await page.fill('input[placeholder="Please input"]', otp);
  await page.click('button:has-text("Confirm")');
  await page.waitForTimeout(10000);
  console.log('[Login] 로그인 완료');
};

// ============================================================
// Fetch reviews via browser's fetch
// ============================================================
const fetchReviewsInBrowser = async (page, spcCds, shop, pageNumber, cursor = '', fromPageNumber = 1) => {
  return await page.evaluate(
    async ({ spcCds, shop, pageNumber, pageSize, cursor, fromPageNumber }) => {
      const params = new URLSearchParams({
        SPC_CDS: spcCds,
        SPC_CDS_VER: '2',
        rating_star: '5,4,3,2,1',
        page_number: String(pageNumber),
        page_size: String(pageSize),
        cursor: String(cursor),
        from_page_number: String(fromPageNumber),
        language: 'en',
        cnsc_shop_id: String(shop.shopId),
        cbsc_shop_region: shop.region,
      });
      const url = `/api/v3/settings/search_shop_rating_comments_new/?${params.toString()}`;
      const resp = await fetch(url);
      return await resp.json();
    },
    { spcCds, shop, pageNumber, pageSize: CONFIG.pageSize, cursor, fromPageNumber }
  );
};

// ============================================================
// Extract next cursor from API response
// API 응답에서 올바른 cursor 값을 추출
// ============================================================
const extractNextCursor = (result) => {
  // 1. page_info에 next_cursor 또는 cursor 필드가 있으면 사용
  const pageInfo = result.data?.page_info;
  if (pageInfo?.next_cursor !== undefined && pageInfo.next_cursor !== null) {
    return String(pageInfo.next_cursor);
  }
  if (pageInfo?.cursor !== undefined && pageInfo.cursor !== null) {
    return String(pageInfo.cursor);
  }
  // 2. data 레벨에 next_cursor가 있으면 사용
  if (result.data?.next_cursor !== undefined && result.data.next_cursor !== null) {
    return String(result.data.next_cursor);
  }
  // 3. fallback: 마지막 아이템의 comment_id
  const list = result.data?.list;
  if (list && list.length > 0) {
    return String(list[list.length - 1].comment_id);
  }
  return '';
};

// ============================================================
// Crawl all reviews for a shop (페이지네이션 수정 + 중복 감지)
// ============================================================
const crawlShopReviews = async (page, spcCds, shop) => {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`[${shop.name}] 리뷰 크롤링 시작...`);
  console.log(`${'='.repeat(60)}`);

  const allReviews = [];
  const seenIds = new Set(); // 유니크 comment_id 추적
  let cursor = '';           // 첫 페이지는 빈 문자열
  let consecutiveDupPages = 0; // 연속 중복 페이지 카운터
  let fromPageNumber = 1;      // cursor가 생성된 페이지 번호

  // --- 첫 페이지 ---
  const firstPage = await fetchReviewsInBrowser(page, spcCds, shop, 1, '');
  if (firstPage.code !== 0) {
    console.error(`[${shop.name}] API 오류:`, firstPage.message);
    return allReviews;
  }

  const totalCount = firstPage.data.page_info.total;
  const totalPages = Math.ceil(totalCount / CONFIG.pageSize);
  console.log(`[${shop.name}] 총 리뷰: ${totalCount.toLocaleString()}건, 총 페이지: ${totalPages.toLocaleString()}`);

  // 첫 페이지 API 응답 구조 로깅 (cursor 필드 디버깅용)
  const pageInfoKeys = Object.keys(firstPage.data.page_info || {});
  const dataKeys = Object.keys(firstPage.data || {}).filter((k) => k !== 'list');
  console.log(`[${shop.name}] page_info keys: [${pageInfoKeys.join(', ')}]`);
  console.log(`[${shop.name}] data keys (except list): [${dataKeys.join(', ')}]`);

  // 첫 페이지 데이터 추가
  for (const item of firstPage.data.list) {
    const id = String(item.comment_id);
    if (!seenIds.has(id)) {
      seenIds.add(id);
      allReviews.push(item);
    }
  }

  cursor = extractNextCursor(firstPage);
  console.log(`[${shop.name}] 첫 페이지 cursor → ${cursor}`);

  // --- 이후 페이지 ---
  for (let pageNumber = 2; pageNumber <= totalPages; pageNumber++) {
    try {
      await new Promise((r) => setTimeout(r, CONFIG.requestDelay));
      const result = await fetchReviewsInBrowser(page, spcCds, shop, pageNumber, cursor, fromPageNumber);

      if (result.code !== 0) {
        console.error(`[${shop.name}] p${pageNumber} 오류: ${result.message}`);
        let retrySuccess = false;
        for (let retry = 0; retry < 3; retry++) {
          await new Promise((r) => setTimeout(r, 2000));
          const retryResult = await fetchReviewsInBrowser(page, spcCds, shop, pageNumber, cursor, fromPageNumber);
          if (retryResult.code === 0) {
            const newIds = [];
            for (const item of retryResult.data.list) {
              const id = String(item.comment_id);
              if (!seenIds.has(id)) {
                seenIds.add(id);
                allReviews.push(item);
                newIds.push(id);
              }
            }
            cursor = extractNextCursor(retryResult);
            fromPageNumber = pageNumber;
            retrySuccess = true;
            break;
          }
        }
        if (!retrySuccess) console.error(`[${shop.name}] p${pageNumber} 재시도 실패, 스킵`);
        continue;
      }

      // 중복 감지: 이번 페이지에서 새로운 ID가 몇 개인지 확인
      let newCount = 0;
      for (const item of result.data.list) {
        const id = String(item.comment_id);
        if (!seenIds.has(id)) {
          seenIds.add(id);
          allReviews.push(item);
          newCount++;
        }
      }

      // 데이터가 없거나 전부 중복이면 카운터 증가
      if (result.data.list.length === 0 || newCount === 0) {
        consecutiveDupPages++;
        if (consecutiveDupPages >= 3) {
          console.warn(`[${shop.name}] p${pageNumber}: 연속 ${consecutiveDupPages}페이지 중복 → 크롤링 조기 종료`);
          console.warn(`[${shop.name}] (API가 더 이상 새 데이터를 반환하지 않음)`);
          break;
        }
      } else {
        consecutiveDupPages = 0; // 새 데이터가 있으면 리셋
      }

      // cursor 업데이트
      cursor = extractNextCursor(result);
      fromPageNumber = pageNumber;

      // 진행 상황 로깅
      if (pageNumber % 50 === 0 || pageNumber === totalPages) {
        const pct = ((pageNumber / totalPages) * 100).toFixed(1);
        console.log(`[${shop.name}] p${pageNumber}/${totalPages} (${pct}%) | 유니크: ${seenIds.size.toLocaleString()} | 이번 페이지 신규: ${newCount}`);
      }

      // 중간 저장 (500 페이지마다)
      if (pageNumber % 500 === 0) {
        const tempPath = path.join(CONFIG.outputDir, `reviews_${shop.region}_temp.json`);
        fs.writeFileSync(tempPath, JSON.stringify(allReviews), 'utf-8');
        console.log(`[${shop.name}] 중간 저장 (${allReviews.length.toLocaleString()}건)`);
      }
    } catch (err) {
      console.error(`[${shop.name}] p${pageNumber} 예외: ${err.message}`);
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  console.log(`[${shop.name}] 완료: 유니크 ${seenIds.size.toLocaleString()}건 (API 보고 총: ${totalCount.toLocaleString()}건)`);
  return allReviews;
};

// ============================================================
// Save helpers
// ============================================================
const escapeCSV = (val) => {
  if (val === null || val === undefined) return '';
  const str = String(val).replace(/"/g, '""');
  return str.includes(',') || str.includes('"') || str.includes('\n') ? `"${str}"` : str;
};

const saveToCSV = (reviews, shop) => {
  const headers = [
    'country', 'comment_id', 'order_sn', 'user_name', 'user_id',
    'rating_star', 'comment', 'product_name', 'product_id', 'model_name',
    'images', 'reply_comment', 'reply_time', 'submit_time', 'submit_date',
    'status', 'low_rating_reasons',
  ];
  const rows = reviews.map((r) => {
    const submitDate = r.submit_time ? new Date(r.submit_time * 1000).toISOString().split('T')[0] : '';
    return [
      shop.name, r.comment_id, r.order_sn, r.user_name, r.user_id,
      r.rating_star, r.comment || '', r.product_name || '', r.product_id,
      r.model_name || '', (r.images || []).join('|'),
      r.reply?.comment || '',
      r.reply?.ctime ? new Date(r.reply.ctime * 1000).toISOString() : '',
      r.submit_time, submitDate, r.status, (r.low_rating_reasons || []).join('|'),
    ].map(escapeCSV).join(',');
  });
  return [headers.join(','), ...rows].join('\n');
};

const formatReviews = (reviews, shop) => reviews.map((r) => ({
  country: shop.name,
  comment_id: r.comment_id,
  order_sn: r.order_sn,
  user_name: r.user_name,
  user_id: r.user_id,
  rating_star: r.rating_star,
  comment: r.comment || '',
  product_name: r.product_name || '',
  product_id: r.product_id,
  model_name: r.model_name || '',
  images: r.images || [],
  reply_comment: r.reply?.comment || '',
  reply_time: r.reply?.ctime ? new Date(r.reply.ctime * 1000).toISOString() : null,
  submit_time: r.submit_time,
  submit_date: r.submit_time ? new Date(r.submit_time * 1000).toISOString().split('T')[0] : '',
  status: r.status,
  low_rating_reasons: r.low_rating_reasons || [],
}));

// ============================================================
// Main
// ============================================================
const main = async () => {
  const startTime = Date.now();
  console.log('Shopee Review Crawler 시작');
  console.log(`대상: ${CONFIG.shops.map((s) => s.name).join(', ')}`);
  console.log('출력: Google Sheets 직접 업로드\n');

  if (!fs.existsSync(CONFIG.outputDir)) fs.mkdirSync(CONFIG.outputDir, { recursive: true });

  // 저장된 auth state 사용
  const hasAuthState = fs.existsSync(CONFIG.authStatePath);
  console.log(`[Auth] 저장된 세션: ${hasAuthState ? '있음' : '없음'}`);

  const browser = await chromium.launch({ headless: true });
  const context = hasAuthState
    ? await browser.newContext({ storageState: CONFIG.authStatePath })
    : await browser.newContext();
  const page = await context.newPage();

  try {
    // 1. Review 페이지 이동
    console.log('[Navigate] Review Management 페이지 이동...');
    await page.goto(CONFIG.reviewUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
    await page.waitForTimeout(5000);

    // 세션 유효성 확인
    const isLoginPage = await page.evaluate(() => window.location.href.includes('/account/signin'));
    if (isLoginPage) {
      console.log('[Auth] 세션 만료됨, 재로그인...');
      await login(page);
      await page.goto(CONFIG.reviewUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await page.waitForTimeout(5000);
      // 새 세션 저장
      await context.storageState({ path: CONFIG.authStatePath });
    }

    // 2. SPC_CDS 추출 & API 테스트
    const spcCds = await page.evaluate(() => document.cookie.match(/SPC_CDS=([^;]+)/)?.[1]);
    console.log(`[API] SPC_CDS: ${spcCds}`);

    const testShop = CONFIG.shops[0]; // 첫 번째 대상 샵으로 테스트
    const testResult = await fetchReviewsInBrowser(page, spcCds, testShop, 1);
    if (testResult.code !== 0) {
      console.error(`[API] 테스트 실패: ${testResult.message}`);
      const allCookies = await context.cookies();
      console.log(`[Debug] 쿠키 수: ${allCookies.length}`);
      console.log(`[Debug] CNSC_SSO: ${allCookies.some(c => c.name === 'CNSC_SSO')}`);
      throw new Error(`API 인증 실패: ${testResult.message}`);
    }
    console.log(`[API] 테스트 성공! ${testShop.name}: ${testResult.data.page_info.total.toLocaleString()}건\n`);

    // 3. 크롤링 + Google Sheets 업로드
    const uploader = new GoogleSheetsUploader();
    let totalUploaded = 0;
    let totalDuplicates = 0;
    const shopResults = {};

    for (const shop of CONFIG.shops) {
      const reviews = await crawlShopReviews(page, spcCds, shop);
      shopResults[shop.name] = reviews.length;
      const formatted = formatReviews(reviews, shop);

      try {
        console.log(`\n[Sheets] ${shop.name}: ${formatted.length.toLocaleString()}건 업로드 시작...`);
        const result = await uploader.upload(formatted);
        console.log(`[Sheets] ${shop.name}: ${result.uploaded.toLocaleString()}건 업로드, ${result.duplicates.toLocaleString()}건 중복 스킵`);
        totalUploaded += result.uploaded;
        totalDuplicates += result.duplicates;
      } catch (sheetsErr) {
        console.error(`[Sheets] ${shop.name} 업로드 실패: ${sheetsErr.message}`);
      }

      // temp 파일 정리
      const tempPath = path.join(CONFIG.outputDir, `reviews_${shop.region}_temp.json`);
      if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
    }

    // 4. 새 세션 저장 (다음 실행용)
    await context.storageState({ path: CONFIG.authStatePath });

    // 5. 요약
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    console.log(`\n${'='.repeat(60)}`);
    console.log('크롤링 완료 요약');
    console.log('='.repeat(60));
    for (const shop of CONFIG.shops) {
      console.log(`  ${shop.name.padEnd(15)} ${(shopResults[shop.name] || 0).toLocaleString()}건 크롤링`);
    }
    console.log(`  ${'Sheets 업로드'.padEnd(14)} ${totalUploaded.toLocaleString()}건 (중복 ${totalDuplicates.toLocaleString()}건 스킵)`);
    console.log(`  소요: ${elapsed}분`);
    console.log('='.repeat(60));
  } catch (err) {
    console.error('크롤링 오류:', err.message);
  } finally {
    await browser.close();
  }
};

main().catch(console.error);
