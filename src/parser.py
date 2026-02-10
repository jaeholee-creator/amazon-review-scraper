"""
Amazon Review Parser

Extracts review data from HTML using BeautifulSoup.
Handles various date formats and locales.
"""

import re
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from typing import List, Dict, Optional


class ReviewParser:
    """Parser for Amazon review HTML."""
    
    # Month name mappings for multiple languages
    MONTH_MAPPINGS = {
        # English
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12,
        # Korean
        '1월': 1, '2월': 2, '3월': 3, '4월': 4,
        '5월': 5, '6월': 6, '7월': 7, '8월': 8,
        '9월': 9, '10월': 10, '11월': 11, '12월': 12,
        # Spanish
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
        # French
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
        'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
        'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12,
    }
    
    def parse_reviews(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Parse all reviews from page HTML.
        
        Args:
            soup: BeautifulSoup object of page HTML
            
        Returns:
            List of review dictionaries
        """
        reviews = []
        review_elements = soup.select('[data-hook="review"]')
        
        for elem in review_elements:
            try:
                review = self._parse_single_review(elem)
                if review:
                    reviews.append(review)
            except Exception as e:
                print(f"⚠️ Failed to parse review: {e}")
                continue
        
        return reviews
    
    def _parse_single_review(self, elem: Tag) -> Optional[Dict]:
        """
        Parse a single review element.
        
        Args:
            elem: BeautifulSoup Tag for review element
            
        Returns:
            Review dictionary or None if parsing fails
        """
        review_id = elem.get('id', '')
        
        # Rating
        rating = self._get_rating(elem)
        
        # Title
        title_elem = elem.select_one('[data-hook="review-title"]')
        title = self._clean_text(title_elem.get_text()) if title_elem else ''
        # Remove rating from title if present
        title = re.sub(r'^별 \d개 중 [\d.]+\s*', '', title).strip()
        title = re.sub(r'^[\d.]+ out of 5 stars\s*', '', title).strip()
        
        # Author
        author_elem = elem.select_one('.a-profile-name')
        author = self._clean_text(author_elem.get_text()) if author_elem else ''
        
        # Date and location
        date_elem = elem.select_one('[data-hook="review-date"]')
        date_text = self._clean_text(date_elem.get_text()) if date_elem else ''
        date_parsed, location = self._parse_date_and_location(date_text)
        
        # Verified purchase
        verified_elem = elem.select_one('[data-hook="avp-badge"]')
        verified = verified_elem is not None
        
        # Content
        content_elem = elem.select_one('[data-hook="review-body"]')
        content = self._clean_text(content_elem.get_text()) if content_elem else ''
        
        # Helpful count
        helpful_elem = elem.select_one('[data-hook="helpful-vote-statement"]')
        helpful_count = self._parse_helpful_count(helpful_elem)
        
        # Images
        image_elems = elem.select('img.review-image-tile')
        image_count = len(image_elems)
        
        return {
            'review_id': review_id,
            'rating': rating,
            'title': title,
            'author': author,
            'date': date_text,
            'date_parsed': date_parsed,
            'location': location,
            'verified_purchase': verified,
            'content': content,
            'helpful_count': helpful_count,
            'image_count': image_count,
            'scraped_at': datetime.now().isoformat()
        }
    
    def _get_rating(self, elem: Tag) -> Optional[float]:
        """Extract rating value from review element."""
        rating_elem = elem.select_one('[data-hook="review-star-rating"], [data-hook="cmps-review-star-rating"]')
        if rating_elem:
            rating_text = rating_elem.get_text()
            # Extract number (e.g., "5.0 out of 5 stars" or "별 5개 중 5.0")
            match = re.search(r'([\d.]+)', rating_text)
            if match:
                return float(match.group(1))
        return None
    
    def _parse_date_and_location(self, date_text: str) -> tuple:
        """
        Parse date and location from review date string.
        
        Examples:
        - "Reviewed in the United States on January 15, 2024"
        - "2024년 1월 15일에 미국에서 리뷰됨"
        
        Returns:
            (datetime, location) tuple
        """
        date_parsed = None
        location = ''
        
        # Try to extract location
        # English: "in the United States"
        loc_match = re.search(r'in (?:the )?([A-Za-z\s]+)(?= on)', date_text)
        if loc_match:
            location = loc_match.group(1).strip()
        # Korean: "미국에서"
        loc_match_kr = re.search(r'([가-힣]+)에서', date_text)
        if loc_match_kr:
            location = loc_match_kr.group(1).strip()
        
        # Try to parse date
        # English format (US): "January 15, 2024"
        date_match = re.search(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', date_text)
        if date_match:
            month_name = date_match.group(1).lower()
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            month = self.MONTH_MAPPINGS.get(month_name)
            if month:
                try:
                    date_parsed = datetime(year, month, day)
                except:
                    pass

        # English format (UK): "29 January 2026"
        if not date_parsed:
            date_match_uk = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', date_text)
            if date_match_uk:
                day = int(date_match_uk.group(1))
                month_name = date_match_uk.group(2).lower()
                year = int(date_match_uk.group(3))
                month = self.MONTH_MAPPINGS.get(month_name)
                if month:
                    try:
                        date_parsed = datetime(year, month, day)
                    except:
                        pass
        
        # Korean format: "2024년 1월 15일"
        date_match_kr = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', date_text)
        if date_match_kr:
            year = int(date_match_kr.group(1))
            month = int(date_match_kr.group(2))
            day = int(date_match_kr.group(3))
            try:
                date_parsed = datetime(year, month, day)
            except:
                pass
        
        return date_parsed, location
    
    def _parse_helpful_count(self, elem: Optional[Tag]) -> int:
        """Parse helpful vote count from element."""
        if not elem:
            return 0
        
        text = elem.get_text()
        # Extract number from "X people found this helpful" or "X명이 유용하다고 평가했습니다"
        match = re.search(r'([\d,]+)', text)
        if match:
            return int(match.group(1).replace(',', ''))
        return 0
    
    def _clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ''
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
