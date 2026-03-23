import requests
from typing import List, Dict, Optional, Any
from config.settings import SLACK_BOT_TOKEN, SLACK_CHANNEL_ID


class SlackNotifier:
    
    def __init__(self, token: Optional[str] = None, channel: Optional[str] = None):
        self.token = token or SLACK_BOT_TOKEN
        self.channel = channel or SLACK_CHANNEL_ID
        self.api_url = 'https://slack.com/api/chat.postMessage'
    
    def send_message(self, text: str, blocks: Optional[List[Dict[str, Any]]] = None) -> bool:
        headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
        }
        
        payload: Dict[str, Any] = {
            'channel': self.channel,
            'text': text,
        }
        
        if blocks:
            payload['blocks'] = blocks
        
        try:
            response = requests.post(self.api_url, headers=headers, json=payload)
            result = response.json()
            
            if not result.get('ok'):
                print(f"Slack API error: {result.get('error')}")
                return False
            return True
            
        except Exception as e:
            print(f"Failed to send Slack message: {e}")
            return False
    
    def send_daily_scrape_report(
        self,
        date_str: str,
        results: List[Dict],
        total_time_seconds: float,
        channel_name: str = '',
    ) -> bool:
        """채널(플랫폼)별 리뷰 수집 결과 리포트.

        Args:
            date_str: 수집 날짜 범위
            results: 제품별 수집 결과 리스트
            total_time_seconds: 총 소요 시간
            channel_name: 채널명 (e.g. 'Amazon US', 'Shopee SG/PH')
        """
        total_reviews = sum(r.get('review_count', 0) for r in results)
        product_count = len(results)
        success_count = sum(1 for r in results if r.get('status') == 'success')
        partial_count = sum(1 for r in results if r.get('status') == 'partial')
        failed_count = sum(1 for r in results if r.get('status') == 'failed')

        status_emoji = '✅' if (failed_count == 0 and partial_count == 0) else '⚠️' if failed_count == 0 else '❌'

        minutes = int(total_time_seconds // 60)
        seconds = int(total_time_seconds % 60)
        time_str = f"{minutes}분 {seconds}초" if minutes > 0 else f"{seconds}초"

        title = f"{status_emoji} {channel_name or 'Review'} Scraper Report"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": title,
                    "emoji": True,
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*채널:*\n{channel_name or 'Unknown'}"},
                    {"type": "mrkdwn", "text": f"*날짜:*\n{date_str}"},
                    {"type": "mrkdwn", "text": f"*제품 수:*\n{product_count}개"},
                    {"type": "mrkdwn", "text": f"*수집 리뷰:*\n{total_reviews}개"},
                ]
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*소요 시간:*\n{time_str}"},
                    {"type": "mrkdwn", "text": f"*결과:*\n✅ {success_count}  ⚠️ {partial_count}  ❌ {failed_count}"},
                ]
            },
        ]

        # 실패/부분 성공 제품만 상세 표시
        problem_products = [r for r in results if r.get('status') != 'success']
        if problem_products:
            blocks.append({"type": "divider"})
            lines = []
            for r in problem_products:
                icon = '⚠️' if r.get('status') == 'partial' else '❌'
                name = r.get('product_name', r.get('asin', '?'))
                err = r.get('error_message', '')
                lines.append(f"{icon} {name}: {r.get('review_count', 0)}개" + (f" ({err})" if err else ""))
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)}
            })

        text = f"{channel_name or 'Review'} Report - {date_str}: {product_count}개 제품, {total_reviews}개 리뷰"
        return self.send_message(text, blocks)
    
    def send_error_alert(self, error_message: str) -> bool:
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 Review Scraper Error",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{error_message}```"
                }
            }
        ]
        
        return self.send_message(f"🚨 Error: {error_message}", blocks)
