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
        total_time_seconds: float
    ) -> bool:
        total_reviews = sum(r.get('review_count', 0) for r in results)
        success_count = sum(1 for r in results if r.get('status') == 'success')
        partial_count = sum(1 for r in results if r.get('status') == 'partial')
        failed_count = sum(1 for r in results if r.get('status') == 'failed')
        
        status_emoji = '✅' if failed_count == 0 else '⚠️' if success_count > 0 else '❌'
        
        minutes = int(total_time_seconds // 60)
        seconds = int(total_time_seconds % 60)
        time_str = f"{minutes}분 {seconds}초" if minutes > 0 else f"{seconds}초"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} BIODANCE Daily Review Report",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*날짜:*\n{date_str}"},
                    {"type": "mrkdwn", "text": f"*총 리뷰:*\n{total_reviews}개"},
                    {"type": "mrkdwn", "text": f"*소요 시간:*\n{time_str}"},
                    {"type": "mrkdwn", "text": f"*결과:*\n✅{success_count} ⚠️{partial_count} ❌{failed_count}"},
                ]
            },
            {"type": "divider"},
        ]
        
        for r in results:
            status = r.get('status', 'unknown')
            if status == 'success':
                status_icon = '✅'
            elif status == 'partial':
                status_icon = '⚠️'
            else:
                status_icon = '❌'
            
            review_count = r.get('review_count', 0)
            product_name = r.get('product_name', r.get('asin', 'Unknown'))
            
            error_msg = r.get('error_message', '')
            detail = f" ({error_msg})" if error_msg else ""
            
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{status_icon} *{product_name}*\n리뷰: {review_count}개{detail}"
                }
            })
        
        text = f"BIODANCE Daily Review Report - {date_str}: 총 {total_reviews}개 리뷰 수집"
        
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
