const { google } = require('googleapis');
const path = require('path');

// ============================================================
// Google Sheets Uploader
// Shopee 리뷰 데이터를 Google Sheets에 업로드 (중복 제거 포함)
// ============================================================

const SHEETS_CONFIG = {
  spreadsheetId: '1NVUVShv5tAveINA9DdB2D21z71L3tF0In5JVK6LYX9s',
  sheetName: 'shopee',
  serviceAccountPath: process.env.GOOGLE_SERVICE_ACCOUNT_PATH
    || path.join(__dirname, 'service-account.json'),
  batchSize: 500,
  maxRetries: 3,
  headers: [
    'country', 'comment_id', 'order_sn', 'user_name', 'user_id',
    'rating_star', 'comment', 'product_name', 'product_id', 'model_name',
    'images', 'reply_comment', 'reply_time', 'submit_time', 'submit_date',
    'status', 'low_rating_reasons',
  ],
};

class GoogleSheetsUploader {
  constructor(config = {}) {
    this.config = { ...SHEETS_CONFIG, ...config };
    this.sheets = null;
  }

  // 서비스 계정 JSON으로 인증
  async authenticate() {
    const auth = new google.auth.GoogleAuth({
      keyFile: this.config.serviceAccountPath,
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    });
    const authClient = await auth.getClient();
    this.sheets = google.sheets({ version: 'v4', auth: authClient });
    console.log('[Sheets] 인증 성공');
  }

  // 시트에 헤더 행이 없으면 추가
  async ensureHeader() {
    const range = `${this.config.sheetName}!A1:Q1`;
    const res = await this.sheets.spreadsheets.values.get({
      spreadsheetId: this.config.spreadsheetId,
      range,
    });

    if (!res.data.values || res.data.values.length === 0) {
      await this.sheets.spreadsheets.values.update({
        spreadsheetId: this.config.spreadsheetId,
        range,
        valueInputOption: 'RAW',
        requestBody: { values: [this.config.headers] },
      });
      console.log('[Sheets] 헤더 행 추가 완료');
    } else {
      console.log('[Sheets] 헤더 행 이미 존재');
    }
  }

  // B열(comment_id) 읽어 Set<string> 반환
  async getExistingCommentIds() {
    const range = `${this.config.sheetName}!B:B`;
    const res = await this.sheets.spreadsheets.values.get({
      spreadsheetId: this.config.spreadsheetId,
      range,
    });

    const ids = new Set();
    if (res.data.values) {
      // 첫 행(헤더) 제외
      for (let i = 1; i < res.data.values.length; i++) {
        const val = res.data.values[i][0];
        if (val) ids.add(String(val));
      }
    }
    console.log(`[Sheets] 기존 comment_id: ${ids.size.toLocaleString()}건`);
    return ids;
  }

  // 로컬 + 원격 중복 제거
  deduplicateAndFilter(reviews, existingIds) {
    const seen = new Set(existingIds);
    const unique = [];

    for (const review of reviews) {
      const id = String(review.comment_id);
      if (!seen.has(id)) {
        seen.add(id);
        unique.push(review);
      }
    }

    const duplicateCount = reviews.length - unique.length;
    if (duplicateCount > 0) {
      console.log(`[Sheets] 중복 제거: ${duplicateCount.toLocaleString()}건 스킵`);
    }
    return unique;
  }

  // 리뷰 객체 → 행 배열 (숫자 ID는 문자열 변환)
  formatRow(review) {
    return [
      review.country || '',
      String(review.comment_id),       // 숫자 ID → 문자열
      String(review.order_sn || ''),
      review.user_name || '',
      String(review.user_id || ''),     // 숫자 ID → 문자열
      review.rating_star ?? '',
      review.comment || '',
      review.product_name || '',
      String(review.product_id || ''),  // 숫자 ID → 문자열
      review.model_name || '',
      Array.isArray(review.images) ? review.images.join('|') : (review.images || ''),
      review.reply_comment || '',
      review.reply_time || '',
      review.submit_time ?? '',
      review.submit_date || '',
      review.status ?? '',
      Array.isArray(review.low_rating_reasons) ? review.low_rating_reasons.join('|') : (review.low_rating_reasons || ''),
    ];
  }

  // 500행/배치, exponential backoff 재시도
  async appendBatchWithRetry(rows) {
    const batches = [];
    for (let i = 0; i < rows.length; i += this.config.batchSize) {
      batches.push(rows.slice(i, i + this.config.batchSize));
    }

    let totalAppended = 0;
    for (let b = 0; b < batches.length; b++) {
      const batch = batches[b];
      let success = false;

      for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
        try {
          await this.sheets.spreadsheets.values.append({
            spreadsheetId: this.config.spreadsheetId,
            range: `${this.config.sheetName}!A:Q`,
            valueInputOption: 'RAW',
            insertDataOption: 'INSERT_ROWS',
            requestBody: { values: batch },
          });
          totalAppended += batch.length;
          success = true;
          break;
        } catch (err) {
          const delay = Math.pow(2, attempt) * 1000;
          console.error(`[Sheets] 배치 ${b + 1}/${batches.length} 실패 (시도 ${attempt + 1}): ${err.message}`);
          if (attempt < this.config.maxRetries - 1) {
            console.log(`[Sheets] ${delay / 1000}초 후 재시도...`);
            await new Promise((r) => setTimeout(r, delay));
          }
        }
      }

      if (!success) {
        throw new Error(`배치 ${b + 1} 업로드 실패 (${this.config.maxRetries}회 재시도 후)`);
      }

      if ((b + 1) % 5 === 0 || b === batches.length - 1) {
        console.log(`[Sheets] 배치 ${b + 1}/${batches.length} 완료 (${totalAppended.toLocaleString()}행)`);
      }
    }

    return totalAppended;
  }

  // 메인 엔트리포인트
  async upload(reviews) {
    if (!reviews || reviews.length === 0) {
      console.log('[Sheets] 업로드할 리뷰가 없습니다.');
      return { uploaded: 0, duplicates: 0, total: 0 };
    }

    console.log(`\n[Sheets] 업로드 시작: ${reviews.length.toLocaleString()}건`);

    if (!this.sheets) await this.authenticate();
    await this.ensureHeader();

    const existingIds = await this.getExistingCommentIds();
    const unique = this.deduplicateAndFilter(reviews, existingIds);

    if (unique.length === 0) {
      console.log('[Sheets] 새로운 리뷰 없음 (전부 중복)');
      return { uploaded: 0, duplicates: reviews.length, total: existingIds.size };
    }

    const rows = unique.map((r) => this.formatRow(r));
    const uploaded = await this.appendBatchWithRetry(rows);

    const result = {
      uploaded,
      duplicates: reviews.length - unique.length,
      total: existingIds.size + uploaded,
    };
    console.log(`[Sheets] 업로드 완료: ${uploaded.toLocaleString()}건 추가, ${result.duplicates.toLocaleString()}건 중복 스킵`);
    console.log(`[Sheets] 시트 총 행 수: ${result.total.toLocaleString()}건`);
    return result;
  }
}

module.exports = { GoogleSheetsUploader, SHEETS_CONFIG };
