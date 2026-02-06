"""
Utility Functions

CSV export, checkpoint management, and progress display.
"""

import csv
import json
import os
from datetime import datetime
from typing import List, Dict, Optional

from config.settings import (
    DATA_DIR,
    REVIEWS_FILE,
    CHECKPOINT_FILE
)


def ensure_data_dir():
    """Create data directory if it doesn't exist."""
    os.makedirs(DATA_DIR, exist_ok=True)


def save_reviews_to_csv(reviews: List[Dict], append: bool = False):
    """
    Save reviews to CSV file.
    
    Args:
        reviews: List of review dictionaries
        append: If True, append to existing file
    """
    if not reviews:
        return
    
    ensure_data_dir()
    
    # Define columns (exclude date_parsed as it's for internal use)
    columns = [
        'review_id', 'rating', 'title', 'author', 'date', 'location',
        'verified_purchase', 'content', 'helpful_count', 'image_count', 'scraped_at'
    ]
    
    mode = 'a' if append else 'w'
    file_exists = os.path.exists(REVIEWS_FILE) and os.path.getsize(REVIEWS_FILE) > 0
    
    with open(REVIEWS_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        
        # Write header only if file is new or not appending
        if not append or not file_exists:
            writer.writeheader()
        
        writer.writerows(reviews)


def save_checkpoint(page_num: int):
    """
    Save current progress to checkpoint file.
    
    Args:
        page_num: Last successfully scraped page number
    """
    ensure_data_dir()
    
    checkpoint = {
        'last_page': page_num,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)
    
    print(f"💾 Checkpoint saved: page {page_num}")


def load_checkpoint() -> Optional[Dict]:
    """
    Load checkpoint from file.
    
    Returns:
        Checkpoint dictionary or None if not found
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    except:
        return None


def clear_checkpoint():
    """Remove checkpoint file."""
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("🗑️ Checkpoint cleared")


def print_progress(page_num: int, reviews_this_page: int, total_reviews: int):
    """
    Print progress information.
    
    Args:
        page_num: Current page number
        reviews_this_page: Number of reviews on current page
        total_reviews: Total reviews collected so far
    """
    print(
        f"📊 Page {page_num:4d} | "
        f"This page: {reviews_this_page:2d} | "
        f"Total: {total_reviews:5d}"
    )


def format_duration(seconds: float) -> str:
    """
    Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "1h 23m 45s")
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, secs = divmod(remainder, 60)
    
    parts = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    
    return ' '.join(parts)
