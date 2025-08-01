"""
Social Media Content Generator - Main Pipeline
Integrates with existing project structure and .env configuration
"""

import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
import json
import pandas as pd
from dotenv import load_dotenv

# Import existing modules
from data_processing import SnowflakeConnector
from ai_contextualizer import ContentGenerator
from batch_processor import BatchProcessor

class SocialContentPipeline:
    def __init__(self):
        """Initialize the social content pipeline using existing infrastructure"""
        # Load environment variables
        load_dotenv()
        
        # Initialize existing components
        self.snowflake_connector = SnowflakeConnector()
        self.content_generator = ContentGenerator()
        self.batch_processor = BatchProcessor()
        
        # Pipeline-specific settings
        self.output_dir = "data/generated_content"
        os.makedirs(self.output_dir, exist_ok=True)
        
    def query_top_events_views(self) -> Dict[str, pd.DataFrame]:
        """Query all 4 Snowflake views for top events data"""
        queries = {
            'base_events': """
                SELECT * FROM adhoc.maggieli.top_events_last_7_days 
                ORDER BY recent_gms_rank
            """,
            'historical_context': """
                SELECT * FROM adhoc.maggieli.top_events_historical_context 
                ORDER BY recent_gms_rank
            """,
            'trend_analysis': """
                SELECT * FROM adhoc.maggieli.top_events_trend_analysis 
                ORDER BY recent_gms_rank
            """,
            'market_rankings': """
                SELECT * FROM adhoc.maggieli.top_events_market_rankings 
                ORDER BY recent_gms_rank
            """
        }
        
        print("📊 Querying Snowflake views...")
        dataframes = {}
        
        for view_name, query in queries.items():
            try:
                df = self.snowflake_connector.execute_query(query)
                dataframes[view_name] = df
                print(f"  ✅ {view_name}: {len(df)} events loaded")
            except Exception as e:
                print(f"  ❌ Failed to load {view_name}: {e}")
                dataframes[view_name] = pd.DataFrame()
        
        return dataframes
    
    def structure_event_data(self, dataframes: Dict[str, pd.DataFrame]) -> List[Dict]:
        """Transform raw dataframes into structured event objects"""
        if dataframes['base_events'].empty:
            print("❌ No base events data available")
            return []
        
        base_df = dataframes['base_events']
        hist_df = dataframes.get('historical_context', pd.DataFrame())
        trend_df = dataframes.get('trend_analysis', pd.DataFrame())
        market_df = dataframes.get('market_rankings', pd.DataFrame())
        
        structured_events = []
        
        print("🔗 Structuring event data...")
        
        for idx, base_row in base_df.iterrows():
            event_id = base_row['EVENT_ID']
            
            # Get matching rows from other views
            hist_row = self._get_matching_row(hist_df, event_id)
            trend_row = self._get_matching_row(trend_df, event_id)
            market_row = self._get_matching_row(market_df, event_id)
            
            # Build comprehensive event object
            event_data = {
                # Basic event info
                'event_id': str(event_id),
                'event_name': str(base_row['EVENT_NAME']),
                'artist_name': str(base_row.get('EVENT_CATEGORY_NAME', 'Unknown')),
                'classified_artist_name': str(base_row.get('CLASSIFIED_ARTIST_NAME', base_row.get('EVENT_CATEGORY_NAME', 'Unknown'))),
                'genre': str(base_row['EVENT_PARENT_CATEGORY_NAME']),
                'subgenre': str(base_row.get('SUBGENRE', '')),
                'venue_city': str(base_row['VENUE_CITY']),
                'venue_country': str(base_row['VENUE_COUNTRY_NAME']),
                'event_date': str(base_row['EVENT_DATE']),
                'rank': int(base_row['RECENT_GMS_RANK']),
                
                # Performance metrics
                'total_gms': self._safe_float(base_row['TOTAL_GMS']),
                'recent_7d_gms': self._safe_float(base_row['RECENT_7D_GMS']),
                'total_tickets': self._safe_int(base_row['TOTAL_TICKETS_SOLD']),
                'avg_ticket_cost': self._safe_float(base_row['AVG_TICKET_COST']),
                'gms_per_ticket': self._safe_float(base_row['GMS_PER_TICKET']),
                'international_pct': self._safe_float(base_row['INTERNATIONAL_GMS_PCT'], 0) * 100,
                'sales_window_days': self._safe_int(base_row.get('TOTAL_SALES_WINDOW_DAYS', 0)),
                
                # Historical context
                'career_context': self._extract_career_context(hist_row),
                'tour_context': self._extract_tour_context(hist_row),
                'genre_context': self._extract_genre_context(hist_row),
                
                # Trend insights
                'trend_insights': self._extract_trend_insights(trend_row),
                'geographic_insights': self._extract_geographic_insights(trend_row),
                'pricing_insights': self._extract_pricing_insights(trend_row),
                
                # Market positioning
                'market_position': self._extract_market_position(market_row),
                
                # Metadata
                'data_timestamp': datetime.now().isoformat(),
                'data_completeness': self._assess_data_completeness(hist_row, trend_row, market_row)
            }
            
            structured_events.append(event_data)
        
        print(f"  ✅ Structured {len(structured_events)} events")
        return structured_events
    
    def _get_matching_row(self, df: pd.DataFrame, event_id) -> Optional[pd.Series]:
        """Get matching row from dataframe by event_id"""
        if df.empty:
            return None
        matching = df[df['EVENT_ID'] == event_id]
        return matching.iloc[0] if len(matching) > 0 else None
    
    def _safe_float(self, value, default=0.0) -> float:
        """Safely convert value to float"""
        try:
            return float(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def _safe_int(self, value, default=0) -> int:
        """Safely convert value to int"""
        try:
            return int(value) if value is not None else default
        except (ValueError, TypeError):
            return default
    
    def _extract_career_context(self, hist_row) -> Dict:
        """Extract career-related insights"""
        if hist_row is None:
            return {}
        
        return {
            'vs_career_avg_multiple': self._safe_float(hist_row.get('VS_CAREER_AVG_MULTIPLE')),
            'vs_career_best_ratio': self._safe_float(hist_row.get('VS_CAREER_BEST_RATIO')),
            'career_total_events': self._safe_int(hist_row.get('CAREER_TOTAL_EVENTS')),
            'career_first_year': self._safe_int(hist_row.get('CAREER_FIRST_YEAR')),
            'career_last_year': self._safe_int(hist_row.get('CAREER_LAST_YEAR')),
            'career_total_gms': self._safe_float(hist_row.get('CAREER_TOTAL_GMS')),
            'career_best_event_gms': self._safe_float(hist_row.get('CAREER_BEST_EVENT_GMS'))
        }
    
    def _extract_tour_context(self, hist_row) -> Dict:
        """Extract tour-related insights"""
        if hist_row is None:
            return {}
        
        return {
            'tour_name': str(hist_row.get('TOUR_NAME', '')) if hist_row.get('TOUR_NAME') else None,
            'vs_tour_avg_multiple': self._safe_float(hist_row.get('VS_TOUR_AVG_MULTIPLE')),
            'tour_total_events': self._safe_int(hist_row.get('TOUR_TOTAL_EVENTS')),
            'tour_total_gms': self._safe_float(hist_row.get('TOUR_TOTAL_GMS'))
        }
    
    def _extract_genre_context(self, hist_row) -> Dict:
        """Extract genre comparison insights"""
        if hist_row is None:
            return {}
        
        return {
            'vs_genre_avg_multiple': self._safe_float(hist_row.get('VS_GENRE_AVG_MULTIPLE')),
            'genre_percentile_bucket': str(hist_row.get('GENRE_PERCENTILE_BUCKET', '')) if hist_row.get('GENRE_PERCENTILE_BUCKET') else None,
            'vs_ytd_avg_multiple': self._safe_float(hist_row.get('VS_YTD_AVG_MULTIPLE'))
        }
    
    def _extract_trend_insights(self, trend_row) -> Dict:
        """Extract trend analysis insights"""
        if trend_row is None:
            return {}
        
        return {
            'gms_multiple': self._safe_float(trend_row.get('GMS_MULTIPLE')),
            'is_gms_spike': bool(trend_row.get('IS_GMS_SPIKE', False)),
            'performance_category': str(trend_row.get('PERFORMANCE_CATEGORY', 'Normal')),
            'price_appreciation_pct': self._safe_float(trend_row.get('PRICE_APPRECIATION_PCT', 0)) * 100
        }
    
    def _extract_geographic_insights(self, trend_row) -> Dict:
        """Extract geographic buyer insights"""
        if trend_row is None:
            return {}
        
        top_countries = []
        for i in range(1, 4):  # Top 3 countries
            country = trend_row.get(f'TOP_BUYER_COUNTRY_{i}')
            pct = trend_row.get(f'TOP_BUYER_COUNTRY_{i}_PCT')
            
            if country and pct is not None:
                top_countries.append({
                    'country': str(country),
                    'percentage': self._safe_float(pct) * 100
                })
        
        return {
            'top_buyer_countries': top_countries,
            'unique_buyer_countries': self._safe_int(trend_row.get('UNIQUE_BUYER_COUNTRIES')),
            'international_appeal_score': len(top_countries)  # Simple score based on diversity
        }
    
    def _extract_pricing_insights(self, trend_row) -> Dict:
        """Extract pricing trend insights"""
        if trend_row is None:
            return {}
        
        return {
            'lifetime_avg_cost': self._safe_float(trend_row.get('LIFETIME_AVG_TICKET_COST')),
            'min_ticket_cost': self._safe_float(trend_row.get('MIN_TICKET_COST')),
            'max_ticket_cost': self._safe_float(trend_row.get('MAX_TICKET_COST')),
            'recent_7d_avg_cost': self._safe_float(trend_row.get('RECENT_7D_AVG_COST')),
            'prior_23d_avg_cost': self._safe_float(trend_row.get('PRIOR_23D_AVG_COST'))
        }
    
    def _extract_market_position(self, market_row) -> Dict:
        """Extract market positioning insights"""
        if market_row is None:
            return {}
        
        return {
            'ytd_overall_rank': self._safe_int(market_row.get('YTD_OVERALL_RANK')),
            'ytd_genre_rank': self._safe_int(market_row.get('YTD_GENRE_RANK')),
            'ytd_overall_tier': str(market_row.get('YTD_OVERALL_TIER', '')) if market_row.get('YTD_OVERALL_TIER') else None,
            'ytd_genre_tier': str(market_row.get('YTD_GENRE_TIER', '')) if market_row.get('YTD_GENRE_TIER') else None,
            'last_7d_market_share_pct': self._safe_float(market_row.get('LAST_7D_MARKET_SHARE_PCT', 0)) * 100,
            'ytd_market_share_pct': self._safe_float(market_row.get('YTD_MARKET_SHARE_PCT', 0)) * 100,
            'premium_multiple': self._safe_float(market_row.get('PREMIUM_MULTIPLE'))
        }
    
    def _assess_data_completeness(self, hist_row, trend_row, market_row) -> Dict:
        """Assess completeness of data for content generation quality"""
        return {
            'has_historical_context': hist_row is not None,
            'has_trend_analysis': trend_row is not None,
            'has_market_positioning': market_row is not None,
            'completeness_score': sum([
                hist_row is not None,
                trend_row is not None, 
                market_row is not None
            ]) / 3  # Score 0-1
        }
    
    def identify_content_angles(self, event_data: Dict) -> List[str]:
        """Identify the most compelling content angles for an event"""
        angles = []
        
        # Career spike angle
        career_multiple = event_data.get('career_context', {}).get('vs_career_avg_multiple', 0)
        if career_multiple >= 5:
            angles.append('major_spike')
        elif career_multiple >= 3:
            angles.append('significant_spike')
        elif career_multiple >= 2:
            angles.append('notable_performance')
        
        # International appeal angle
        if event_data.get('international_pct', 0) > 40:
            angles.append('international_phenomenon')
        elif event_data.get('international_pct', 0) > 25:
            angles.append('international_appeal')
        
        # Market leadership angle
        genre_rank = event_data.get('market_position', {}).get('ytd_genre_rank', 999)
        if genre_rank <= 3:
            angles.append('genre_leader')
        elif genre_rank <= 10:
            angles.append('top_performer')
        
        # Pricing momentum angle
        price_appreciation = event_data.get('trend_insights', {}).get('price_appreciation_pct', 0)
        if price_appreciation > 30:
            angles.append('pricing_surge')
        elif price_appreciation > 15:
            angles.append('demand_indicator')
        
        # Tour context angle
        if event_data.get('tour_context', {}).get('tour_name'):
            tour_multiple = event_data.get('tour_context', {}).get('vs_tour_avg_multiple', 0)
            if tour_multiple > 1.5:
                angles.append('tour_standout')
        
        # Default angles if nothing stands out
        if not angles:
            if event_data.get('rank', 10) <= 5:
                angles.append('top_performance')
            else:
                angles.append('trending_event')
        
        return angles[:3]  # Return top 3 angles
    
    def generate_content_for_events(self, events: List[Dict]) -> List[Dict]:
        """Generate social media content for all events"""
        print("✍️ Generating social media content...")
        
        all_content = []
        
        for event in events:
            display_name = event['classified_artist_name'] if event['classified_artist_name'] not in ['Unknown', 'None', None, 'nan'] else event['artist_name']
            print(f"  Processing: {display_name} - {event['event_name']}")
            
            # Identify compelling angles
            angles = self.identify_content_angles(event)
            
            # Generate content for each angle
            for angle in angles:
                try:
                    content = self.content_generator.create_social_post(
                        event_data=event,
                        content_angle=angle,
                        platform='twitter'  # Can be made configurable
                    )
                    
                    content_item = {
                        'event_id': event['event_id'],
                        'artist_name': event['classified_artist_name'],
                        'event_name': event['event_name'],
                        'content_angle': angle,
                        'platform': 'tiktok',
                        
                        # New dual content format
                        'visual_text': content['visual_text'],
                        'caption': content['caption'],
                        
                        'event_data': event,
                        'generated_at': datetime.now().isoformat(),
                        'data_quality_score': event['data_completeness']['completeness_score']
                    }
                    
                    all_content.append(content_item)
                    print(f"    ✅ Generated {angle} content")
                    
                except Exception as e:
                    print(f"    ❌ Failed to generate {angle} content: {e}")
        
        return all_content
    
    def save_generated_content(self, content: List[Dict], filename: str = None) -> Dict[str, str]:
        """Save generated content to both JSON and text files with metadata"""
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            json_filename = f"{self.output_dir}/social_content_{timestamp}.json"
            text_filename = f"{self.output_dir}/social_content_{timestamp}.txt"
        else:
            base_name = filename.replace('.json', '')
            json_filename = f"{base_name}.json"
            text_filename = f"{base_name}.txt"
        
        # Add summary metadata
        output_data = {
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'total_content_pieces': len(content),
                'unique_events': len(set(item['event_id'] for item in content)),
                'content_angles': list(set(item['content_angle'] for item in content)),
                'average_data_quality': sum(item['data_quality_score'] for item in content) / len(content) if content else 0
            },
            'content': content
        }
        
        # Save JSON file
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        # Create human-readable text export
        self._save_readable_text(output_data, text_filename)
        
        print(f"💾 Content saved to:")
        print(f"  📄 JSON: {json_filename}")
        print(f"  📝 Text: {text_filename}")
        
        return {
            'json_file': json_filename,
            'text_file': text_filename
        }
    
    def _save_readable_text(self, output_data: Dict, filename: str):
        """Save content in human-readable text format"""
        with open(filename, 'w', encoding='utf-8') as f:
            # Write header
            f.write("🎵 SOCIAL MEDIA CONTENT REPORT\n")
            f.write("=" * 60 + "\n\n")
            
            # Write metadata
            metadata = output_data['metadata']
            f.write("📊 SUMMARY\n")
            f.write("-" * 30 + "\n")
            f.write(f"Generated: {metadata['generated_at']}\n")
            f.write(f"Total Content Pieces: {metadata['total_content_pieces']}\n")
            f.write(f"Unique Events: {metadata['unique_events']}\n")
            f.write(f"Content Angles: {', '.join(metadata['content_angles'])}\n")
            f.write(f"Average Data Quality: {metadata['average_data_quality']:.1%}\n\n")
            
            # Group content by artist
            content_by_artist = {}
            for item in output_data['content']:
                artist = item['artist_name']
                if artist not in content_by_artist:
                    content_by_artist[artist] = []
                content_by_artist[artist].append(item)
            
            # Write content by artist
            f.write("🎤 CONTENT BY ARTIST\n")
            f.write("=" * 60 + "\n\n")
            
            for artist, items in content_by_artist.items():
                f.write(f"🎭 {artist.upper()}\n")
                f.write("-" * 40 + "\n")
                
                for i, item in enumerate(items, 1):
                    f.write(f"\n[{i}] Content Angle: {item['content_angle'].upper()}\n")
                    f.write(f"Event: {item['event_name']}\n")
                    f.write(f"Platform: {item['platform'].title()}\n")
                    
                    # Handle different content formats
                    if 'visual_text' in item and 'caption' in item:
                        # New dual format
                        f.write("Visual Text:\n")
                        visual_lines = item['visual_text'].split('\n')
                        for line in visual_lines:
                            f.write(f"  {line}\n")
                        
                        f.write("\nCaption:\n")
                        caption_lines = item['caption'].split('\n')
                        for line in caption_lines:
                            f.write(f"  {line}\n")
                    elif 'content' in item:
                        # Legacy format
                        f.write("Content:\n")
                        content_lines = item['content'].split('\n')
                        for line in content_lines:
                            f.write(f"  {line}\n")
                    
                    # Add event metrics if available
                    event_data = item.get('event_data', {})
                    if event_data:
                        f.write(f"\n📈 Event Metrics:\n")
                        f.write(f"  • Rank: #{event_data.get('rank', 'N/A')}\n")
                        f.write(f"  • Location: {event_data.get('venue_city', 'N/A')}, {event_data.get('venue_country', 'N/A')}\n")
                        f.write(f"  • Genre: {event_data.get('genre', 'N/A')}\n")
                        
                        recent_gms = event_data.get('recent_7d_gms', 0)
                        if recent_gms:
                            f.write(f"  • Recent 7d GMS: ${recent_gms:,.0f}\n")
                        
                        career_multiple = event_data.get('career_context', {}).get('vs_career_avg_multiple', 0)
                        if career_multiple:
                            f.write(f"  • vs Career Avg: {career_multiple:.1f}x\n")
                    
                    f.write("\n" + "~" * 50 + "\n")
                
                f.write("\n\n")
    
    def run_pipeline(self) -> str:
        """Execute the complete social media content generation pipeline"""
        print("🚀 Starting Social Media Content Generation Pipeline")
        print(f"📁 Output directory: {self.output_dir}")
        
        try:
            # Step 1: Query Snowflake views
            dataframes = self.query_top_events_views()
            
            # Step 2: Structure event data
            events = self.structure_event_data(dataframes)
            
            if not events:
                print("❌ No events to process")
                return None
            
            # Step 3: Generate content
            content = self.generate_content_for_events(events)
            
            # Step 4: Save results
            output_files = self.save_generated_content(content)
            
            # Step 5: Summary
            print(f"\n🎉 Pipeline completed successfully!")
            print(f"📊 Generated {len(content)} pieces of content for {len(events)} events")
            print(f"💾 Output saved to:")
            print(f"  📄 JSON: {output_files['json_file']}")
            print(f"  📝 Text: {output_files['text_file']}")
            
            return output_files
            
        except Exception as e:
            print(f"❌ Pipeline failed: {e}")
            raise


def main():
    """Main entry point with enhanced functionality"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate social media content from top events')
    parser.add_argument('--test-connection', action='store_true', help='Test Snowflake connection only')
    parser.add_argument('--dry-run', action='store_true', help='Process data without calling OpenAI API')
    parser.add_argument('--max-events', type=int, default=10, help='Maximum events to process')
    parser.add_argument('--output-dir', type=str, default='data/generated_content', help='Output directory')
    
    args = parser.parse_args()
    
    pipeline = SocialContentPipeline()
    
    # Test connection if requested
    if args.test_connection:
        print("🔌 Testing Snowflake connection...")
        if pipeline.snowflake_connector.test_connection():
            print("✅ Connection successful!")
            pipeline.snowflake_connector.validate_views_exist()
        else:
            print("❌ Connection failed!")
        return
    
    # Set output directory
    pipeline.output_dir = args.output_dir
    
    try:
        if args.dry_run:
            print("🧪 Running in dry-run mode (no API calls)")
            # Just process data without generating content
            dataframes = pipeline.query_top_events_views()
            events = pipeline.structure_event_data(dataframes)
            
            print(f"📊 Would process {len(events)} events")
            for i, event in enumerate(events[:5], 1):  # Show first 5
                angles = pipeline.identify_content_angles(event)
                display_name = event['classified_artist_name'] if event['classified_artist_name'] not in ['Unknown', 'None', None, 'nan'] else event['artist_name']
                print(f"  {i}. {display_name} - {len(angles)} content angles")
        else:
            # Full pipeline execution
            output_files = pipeline.run_pipeline()
            
            if output_files:
                print(f"\n✅ Social media content generation complete!")
                print(f"📁 Check your outputs:")
                print(f"  📄 JSON: {output_files['json_file']}")
                print(f"  📝 Text: {output_files['text_file']}")
                
                # Additional post-processing options
                print(f"\n🎯 Next steps:")
                print(f"  1. Review generated content in the text file")
                print(f"  2. Use visual_text for TikTok/Instagram assets")
                print(f"  3. Use caption for post descriptions")
                print(f"  4. Set up Zapier webhook for automation")
            else:
                print("\n❌ Content generation failed")
                
    except KeyboardInterrupt:
        print("\n⏹️  Process interrupted by user")
    except Exception as e:
        print(f"\n❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()