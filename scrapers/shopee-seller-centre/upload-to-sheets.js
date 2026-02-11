const fs = require('fs');
const path = require('path');
const { GoogleSheetsUploader } = require('./google-sheets');

// ============================================================
// 독립 업로드 스크립트
// output/*.json 파일을 Google Sheets에 업로드 (중복 제거)
// 사용: node upload-to-sheets.js [--file reviews_sg.json]
// ============================================================

const OUTPUT_DIR = path.join(__dirname, 'output');

const loadReviewsFromJSON = (filePath) => {
  if (!fs.existsSync(filePath)) {
    console.warn(`[Load] 파일 없음: ${filePath}`);
    return [];
  }
  const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
  console.log(`[Load] ${path.basename(filePath)}: ${data.length.toLocaleString()}건`);
  return data;
};

const main = async () => {
  const startTime = Date.now();
  console.log('Shopee Review → Google Sheets 업로드');
  console.log('='.repeat(50));

  // --file 옵션으로 특정 파일만 업로드 가능
  const fileArg = process.argv.find((a, i) => process.argv[i - 1] === '--file');

  let allReviews = [];

  if (fileArg) {
    const filePath = path.join(OUTPUT_DIR, fileArg);
    allReviews = loadReviewsFromJSON(filePath);
  } else {
    // 개별 국가 파일 로드 (reviews_all.json은 중복이므로 사용 안 함)
    const regionFiles = ['reviews_sg.json', 'reviews_tw.json', 'reviews_ph.json'];
    for (const file of regionFiles) {
      const filePath = path.join(OUTPUT_DIR, file);
      const reviews = loadReviewsFromJSON(filePath);
      allReviews.push(...reviews);
    }
  }

  if (allReviews.length === 0) {
    console.log('업로드할 리뷰가 없습니다.');
    return;
  }

  // 로컬 중복 제거 (같은 comment_id가 여러 파일에 있을 수 있음)
  const seen = new Set();
  const dedupedReviews = [];
  for (const review of allReviews) {
    const id = String(review.comment_id);
    if (!seen.has(id)) {
      seen.add(id);
      dedupedReviews.push(review);
    }
  }
  console.log(`\n[Dedup] 로컬 중복 제거: ${allReviews.length.toLocaleString()} → ${dedupedReviews.length.toLocaleString()}건`);

  // Sheets 업로드
  const uploader = new GoogleSheetsUploader();
  const result = await uploader.upload(dedupedReviews);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(50)}`);
  console.log(`완료 (${elapsed}초)`);
  console.log(`  업로드: ${result.uploaded.toLocaleString()}건`);
  console.log(`  중복 스킵: ${result.duplicates.toLocaleString()}건`);
  console.log(`  시트 총합: ${result.total.toLocaleString()}건`);
  console.log('='.repeat(50));
};

main().catch((err) => {
  console.error('업로드 오류:', err.message);
  process.exit(1);
});
