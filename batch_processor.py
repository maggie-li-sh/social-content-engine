"""
Batch Processor - Enhanced for Social Media Content Generation
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class BatchProcessor:
    def __init__(self, max_workers: int = 3, rate_limit_delay: float = 1.0):
        """
        Initialize batch processor for social content generation
        
        Args:
            max_workers: Maximum number of concurrent API calls
            rate_limit_delay: Delay between API calls in seconds
        """
        self.max_workers = max_workers
        self.rate_limit_delay = rate_limit_delay
        self.processed_count = 0
        self.error_count = 0
        self.start_time = None
        
    def process_events_batch(self, events: List[Dict], content_generator, 
                           max_content_per_event: int = 2) -> List[Dict]:
        """
        Process multiple events in batches to generate social content
        
        Args:
            events: List of event dictionaries
            content_generator: ContentGenerator instance
            max_content_per_event: Maximum pieces of content per event
            
        Returns:
            List of generated content items
        """
        print(f"🔄 Starting batch processing for {len(events)} events")
        self.start_time = datetime.now()
        self.processed_count = 0
        self.error_count = 0
        
        all_content = []
        
        # Process events with rate limiting
        for i, event in enumerate(events, 1):
            print(f"  Processing event {i}/{len(events)}: {event.get('classified_artist_name', 'Unknown')}")
            
            try:
                # Generate content for this event
                event_content = self._process_single_event(
                    event, content_generator, max_content_per_event
                )
                all_content.extend(event_content)
                self.processed_count += 1
                
                # Rate limiting
                if i < len(events):  # Don't delay after the last item
                    time.sleep(self.rate_limit_delay)
                    
            except Exception as e:
                print(f"    ❌ Error processing event {event.get('event_id', 'unknown')}: {e}")
                self.error_count += 1
                continue
        
        self._print_batch_summary(all_content)
        return all_content
    
    def _process_single_event(self, event: Dict, content_generator, 
                             max_content: int) -> List[Dict]:
        """Process a single event to generate social content"""
        content_items = []
        
        # Identify content angles based on event data
        angles = self._identify_content_angles(event)
        
        # Limit angles to max_content
        selected_angles = angles[:max_content]
        
        for angle in selected_angles:
            try:
                # Generate content for this angle (now returns dict with visual_text + caption)
                content_result = content_generator.create_social_post(
                    event_data=event,
                    content_angle=angle,
                    platform='tiktok'  # Changed to TikTok only
                )
                
                content_item = {
                    'event_id': event['event_id'],
                    'artist_name': event['classified_artist_name'],
                    'event_name': event['event_name'],
                    'venue_location': f"{event['venue_city']}, {event['venue_country']}",
                    'genre': event['genre'],
                    'rank': event['rank'],
                    'content_angle': angle,
                    'platform': content_result['platform'],
                    
                    # New dual content format
                    'visual_text': content_result['visual_text'],
                    'caption': content_result['caption'],
                    
                    'generated_at': datetime.now().isoformat(),
                    'event_metrics': {
                        'rank': event['rank'],
                        'international_pct': event['international_pct'],
                        'vs_career_avg_multiple': event.get('career_context', {}).get('vs_career_avg_multiple', 1),
                        'genre_rank': event.get('market_position', {}).get('ytd_genre_rank'),
                        'performance_category': event.get('trend_insights', {}).get('performance_category', 'Normal'),
                        'genre_percentile': event.get('genre_context', {}).get('genre_percentile_bucket', 'Unknown')
                    },
                    'data_quality_score': event.get('data_completeness', {}).get('completeness_score', 0),
                    'content_priority': self._calculate_content_priority(event, angle)
                }
                
                content_items.append(content_item)
                print(f"    ✅ Generated {angle} content")
                
            except Exception as e:
                print(f"    ❌ Failed to generate {angle} content: {e}")
                continue
        
        return content_items
    
    def _identify_content_angles(self, event: Dict) -> List[str]:
        """Identify compelling content angles for an event"""
        angles = []
        
        # Performance spike angles
        career_multiple = event.get('career_context', {}).get('vs_career_avg_multiple', 0)
        if career_multiple >= 5:
            angles.append('major_spike')
        elif career_multiple >= 3:
            angles.append('significant_spike')
        elif career_multiple >= 2:
            angles.append('notable_performance')
        
        # International appeal angles
        intl_pct = event.get('international_pct', 0)
        if intl_pct > 40:
            angles.append('international_phenomenon')
        elif intl_pct > 25:
            angles.append('international_appeal')
        
        # Market ranking angles
        genre_rank = event.get('market_position', {}).get('ytd_genre_rank', 999)
        if genre_rank <= 3:
            angles.append('genre_leader')
        elif genre_rank <= 10:
            angles.append('top_performer')
        
        # Pricing trend angles
        price_appreciation = event.get('trend_insights', {}).get('price_appreciation_pct', 0)
        if price_appreciation > 30:
            angles.append('pricing_surge')
        elif price_appreciation > 15:
            angles.append('demand_indicator')
        
        # Tour context angles
        tour_multiple = event.get('tour_context', {}).get('vs_tour_avg_multiple', 0)
        if tour_multiple > 1.5 and event.get('tour_context', {}).get('tour_name'):
            angles.append('tour_standout')
        
        # Default angles for high-ranking events
        if not angles:
            if event.get('rank', 10) <= 5:
                angles.append('top_performance')
            else:
                angles.append('trending_event')
        
        return angles
    
    def _calculate_content_priority(self, event: Dict, angle: str) -> int:
        """Calculate priority score for content item (1-10, 10 = highest)"""
        priority = 5  # Base priority
        
        # Boost for high-performing events (using relative metrics, not GMS)
        rank = event.get('rank', 10)
        if rank <= 3:
            priority += 3
        elif rank <= 5:
            priority += 2
        elif rank <= 10:
            priority += 1
        
        # Boost for compelling angles
        high_impact_angles = ['major_spike', 'international_phenomenon', 'genre_leader', 'pricing_surge']
        if angle in high_impact_angles:
            priority += 2
        
        # Boost for complete data
        data_score = event.get('data_completeness', {}).get('completeness_score', 0)
        if data_score >= 0.8:
            priority += 1
        
        # Boost for high career multiples (viral potential)
        career_multiple = event.get('career_context', {}).get('vs_career_avg_multiple', 1)
        if career_multiple >= 5:
            priority += 2
        elif career_multiple >= 3:
            priority += 1
        
        return min(priority, 10)  # Cap at 10
    
    def _print_batch_summary(self, content: List[Dict]):
        """Print summary of batch processing results"""
        end_time = datetime.now()
        duration = end_time - self.start_time if self.start_time else timedelta(0)
        
        print(f"\n📊 Batch Processing Summary:")
        print(f"  ⏱️  Duration: {duration}")
        print(f"  ✅ Events processed: {self.processed_count}")
        print(f"  ❌ Events failed: {self.error_count}")
        print(f"  📝 Content items generated: {len(content)}")
        
        if content:
            # Analyze content by angle
            angle_counts = {}
            priority_scores = []
            
            for item in content:
                angle = item['content_angle']
                angle_counts[angle] = angle_counts.get(angle, 0) + 1
                priority_scores.append(item['content_priority'])
            
            print(f"  📈 Average content priority: {sum(priority_scores) / len(priority_scores):.1f}")
            print(f"  🎯 Content angles generated:")
            for angle, count in sorted(angle_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"     {angle}: {count}")
    
    def save_content_with_metadata(self, content: List[Dict], output_dir: str = "data/generated_content") -> str:
        """Save generated content with comprehensive metadata"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{output_dir}/social_content_{timestamp}.json"
        
        # Sort content by priority (highest first)
        sorted_content = sorted(content, key=lambda x: x['content_priority'], reverse=True)
        
        # Generate metadata
        metadata = self._generate_content_metadata(sorted_content)
        
        # Create output structure
        output_data = {
            'metadata': metadata,
            'content': sorted_content
        }
        
        # Save to file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"💾 Content saved to {filename}")
        return filename
    
    def _generate_content_metadata(self, content: List[Dict]) -> Dict:
        """Generate comprehensive metadata for content batch"""
        if not content:
            return {}
        
        # Basic counts
        total_items = len(content)
        unique_events = len(set(item['event_id'] for item in content))
        unique_artists = len(set(item['artist_name'] for item in content))
        
        # Content analysis
        angles_distribution = {}
        priority_distribution = {}
        genre_distribution = {}
        
        for item in content:
            # Angle distribution
            angle = item['content_angle']
            angles_distribution[angle] = angles_distribution.get(angle, 0) + 1
            
            # Priority distribution
            priority = item['content_priority']
            priority_distribution[priority] = priority_distribution.get(priority, 0) + 1
            
            # Genre distribution
            genre = item['genre']
            genre_distribution[genre] = genre_distribution.get(genre, 0) + 1
        
        # Quality metrics
        avg_priority = sum(item['content_priority'] for item in content) / total_items
        avg_data_quality = sum(item['data_quality_score'] for item in content) / total_items
        high_priority_items = sum(1 for item in content if item['content_priority'] >= 8)
        
        # Top performers
        top_events_by_gms = sorted(
            [{'artist': item['artist_name'], 'event': item['event_name'], 
              'gms': item['event_metrics']['recent_7d_gms']} for item in content],
            key=lambda x: x['gms'], reverse=True
        )[:5]
        
        return {
            'generation_timestamp': datetime.now().isoformat(),
            'batch_summary': {
                'total_content_items': total_items,
                'unique_events': unique_events,
                'unique_artists': unique_artists,
                'processing_duration': str(datetime.now() - self.start_time) if self.start_time else None
            },
            'quality_metrics': {
                'average_content_priority': round(avg_priority, 2),
                'average_data_quality_score': round(avg_data_quality, 2),
                'high_priority_items': high_priority_items,
                'high_priority_percentage': round(high_priority_items / total_items * 100, 1)
            },
            'content_distribution': {
                'by_angle': dict(sorted(angles_distribution.items(), key=lambda x: x[1], reverse=True)),
                'by_priority': dict(sorted(priority_distribution.items(), reverse=True)),
                'by_genre': dict(sorted(genre_distribution.items(), key=lambda x: x[1], reverse=True))
            },
            'top_events_by_gms': top_events_by_gms,
            'recommended_posting_order': [
                item['event_id'] for item in content[:10]  # Top 10 by priority
            ]
        }
    
    def filter_content_by_criteria(self, content: List[Dict], 
                                 min_priority: int = 6,
                                 max_items: int = None,
                                 preferred_angles: List[str] = None) -> List[Dict]:
        """Filter content based on quality criteria"""
        filtered = content.copy()
        
        # Filter by minimum priority
        filtered = [item for item in filtered if item['content_priority'] >= min_priority]
        
        # Filter by preferred angles if specified
        if preferred_angles:
            filtered = [item for item in filtered if item['content_angle'] in preferred_angles]
        
        # Sort by priority (highest first)
        filtered = sorted(filtered, key=lambda x: x['content_priority'], reverse=True)
        
        # Limit number of items
        if max_items and len(filtered) > max_items:
            filtered = filtered[:max_items]
        
        print(f"📋 Filtered content: {len(filtered)} items (from {len(content)} original)")
        return filtered
    
    def create_posting_schedule(self, content: List[Dict], 
                              posts_per_day: int = 3,
                              start_date: datetime = None) -> Dict:
        """Create a suggested posting schedule for social content"""
        if not content:
            return {}
        
        if start_date is None:
            start_date = datetime.now()
        
        # Sort content by priority
        sorted_content = sorted(content, key=lambda x: x['content_priority'], reverse=True)
        
        schedule = {}
        current_date = start_date
        
        for i, item in enumerate(sorted_content):
            day_key = current_date.strftime('%Y-%m-%d')
            
            if day_key not in schedule:
                schedule[day_key] = []
            
            # Add item to current day
            schedule[day_key].append({
                'post_time': (current_date + timedelta(hours=(len(schedule[day_key]) * 8))).strftime('%H:%M'),
                'content_id': f"{item['event_id']}_{item['content_angle']}",
                'artist': item['artist_name'],
                'event': item['event_name'],
                'angle': item['content_angle'],
                'priority': item['content_priority'],
                'content_preview': item['content'][:100] + "..." if len(item['content']) > 100 else item['content']
            })
            
            # Move to next day if we've reached the daily limit
            if len(schedule[day_key]) >= posts_per_day:
                current_date += timedelta(days=1)
        
        return {
            'schedule': schedule,
            'total_days': len(schedule),
            'total_posts': len(sorted_content),
            'posts_per_day': posts_per_day,
            'start_date': start_date.strftime('%Y-%m-%d')
        }
    
    def export_for_zapier(self, content: List[Dict], max_items: int = 20) -> Dict:
        """Export content in format suitable for Zapier automation"""
        # Take top priority items
        top_content = sorted(content, key=lambda x: x['content_priority'], reverse=True)[:max_items]
        
        zapier_data = {
            'webhook_data': {
                'timestamp': datetime.now().isoformat(),
                'content_count': len(top_content),
                'posts': []
            }
        }
        
        for item in top_content:
            zapier_post = {
                'id': f"{item['event_id']}_{item['content_angle']}",
                'artist_name': item['artist_name'],
                'event_name': item['event_name'],
                'venue_location': item['venue_location'],
                
                # New dual content format
                'visual_text': item['visual_text'],
                'caption': item['caption'],
                
                'content_angle': item['content_angle'],
                'priority_score': item['content_priority'],
                'platform': item['platform'],
                'hashtags': self._generate_hashtags(item),
                'metrics': {
                    'rank': item['event_metrics']['rank'],
                    'international_pct': item['event_metrics']['international_pct'],
                    'career_multiple': item['event_metrics']['vs_career_avg_multiple'],
                    'performance_category': item['event_metrics']['performance_category']
                }
            }
            zapier_data['webhook_data']['posts'].append(zapier_post)
        
        return zapier_data
    
    def _generate_hashtags(self, content_item: Dict) -> List[str]:
        """Generate relevant hashtags for social media posts"""
        hashtags = ['#livemusic', '#concerts']
        
        # Add genre-based hashtags
        genre = content_item['genre'].lower().replace(' ', '')
        if genre:
            hashtags.append(f'#{genre}')
        
        # Add angle-based hashtags
        angle_hashtags = {
            'major_spike': ['#trending', '#breakingnews'],
            'international_phenomenon': ['#global', '#international'],
            'genre_leader': ['#leader', '#dominating'],
            'pricing_surge': ['#demand', '#hottickets'],
            'tour_standout': ['#tour', '#standout']
        }
        
        angle = content_item['content_angle']
        if angle in angle_hashtags:
            hashtags.extend(angle_hashtags[angle])
        
        # Add artist name hashtag (cleaned)
        artist = content_item['artist_name'].replace(' ', '').replace('&', 'and')
        if len(artist) <= 20:  # Reasonable hashtag length
            hashtags.append(f'#{artist}')
        
        return hashtags[:8]  # Limit to 8 hashtags


def main():
    """Test the batch processor functionality"""
    from data_processing import DataProcessor
    from ai_contextualizer import ContentGenerator
    
    # Initialize components
    processor = DataProcessor()
    content_generator = ContentGenerator()
    batch_processor = BatchProcessor()
    
    print("🔄 Testing Batch Processor...")
    
    # Get sample data
    try:
        raw_data = processor.snowflake.get_top_events_data()
        events = processor.process_event_data(raw_data)
        
        if not events:
            print("❌ No events to process")
            return
        
        # Limit to first 3 events for testing
        test_events = events[:3]
        
        # Process batch
        content = batch_processor.process_events_batch(
            test_events, content_generator, max_content_per_event=2
        )
        
        # Save results
        output_file = batch_processor.save_content_with_metadata(content)
        
        # Create posting schedule
        schedule = batch_processor.create_posting_schedule(content, posts_per_day=2)
        print(f"\n📅 Created {schedule['total_days']}-day posting schedule")
        
        # Export for Zapier
        zapier_data = batch_processor.export_for_zapier(content, max_items=5)
        print(f"📤 Prepared {len(zapier_data['webhook_data']['posts'])} posts for Zapier")
        
        processor.snowflake.close_connection()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")


if __name__ == "__main__":
    main()