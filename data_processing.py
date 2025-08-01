"""
Data Processing - Enhanced with Snowflake Integration for Social Content Pipeline
Uses your existing Snowflake connection pattern
"""

import os
import pandas as pd
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

def is_running_locally():
    return os.environ.get('IS_LOCAL_DEV', '') == '1'

IS_LOCAL_DEV = is_running_locally()

if IS_LOCAL_DEV:
    import snowflake.connector
else:
    from snowflake.snowpark.context import get_active_session

class SnowflakeConnector:
    def __init__(self):
        """Initialize Snowflake connection using your existing pattern"""
        self.conn = None
        self.is_local = IS_LOCAL_DEV
        
    def get_connection(self):
        """Create and return a Snowflake connection or session using your pattern"""
        if self.is_local:
            # Validate required environment variables
            required_vars = [
                'SNOWFLAKE_ACCOUNT', 
                'SNOWFLAKE_USER',
                'SNOWFLAKE_AUTHENTICATOR',
                'SNOWFLAKE_WAREHOUSE',
                'SNOWFLAKE_DATABASE',
                'SNOWFLAKE_SCHEMA',
                'SNOWFLAKE_ROLE'
            ]
            missing_vars = [var for var in required_vars if not os.environ.get(var)]
            
            if missing_vars:
                raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
            
            return snowflake.connector.connect(
                account=os.environ.get('SNOWFLAKE_ACCOUNT'),
                user=os.environ.get('SNOWFLAKE_USER'),
                authenticator=os.environ.get('SNOWFLAKE_AUTHENTICATOR'),
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
                database=os.environ.get('SNOWFLAKE_DATABASE'),
                schema=os.environ.get('SNOWFLAKE_SCHEMA'),
                role=os.environ.get('SNOWFLAKE_ROLE')
            )
        else:
            # In Snowflake environment, use the session
            return get_active_session()
        
    def connect(self):
        """Establish connection to Snowflake"""
        try:
            self.conn = self.get_connection()
            print("✅ Connected to Snowflake")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Snowflake: {e}")
            return False
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute a query and return results as DataFrame"""
        if not self.conn:
            if not self.connect():
                raise Exception("Cannot establish Snowflake connection")
        
        try:
            if self.is_local:
                # Use traditional cursor approach for local development
                cursor = self.conn.cursor()
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch all results
                results = cursor.fetchall()
                
                # Create DataFrame
                df = pd.DataFrame(results, columns=columns)
                cursor.close()
                
                return df
            else:
                # Use Snowpark session for Snowflake environment
                df = self.conn.sql(query).to_pandas()
                return df
                
        except Exception as e:
            print(f"❌ Query execution failed: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection"""
        try:
            test_query = "SELECT CURRENT_TIMESTAMP() as test_timestamp"
            result = self.execute_query(test_query)
            timestamp = result.iloc[0]['TEST_TIMESTAMP'] if self.is_local else result.iloc[0]['test_timestamp']
            print(f"✅ Connection test successful. Timestamp: {timestamp}")
            return True
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False
    
    def get_top_events_data(self) -> Dict[str, pd.DataFrame]:
        """Query all social content pipeline views"""
        views = {
            'base_events': 'adhoc.maggieli.top_events_last_7_days',
            'historical_context': 'adhoc.maggieli.top_events_historical_context',
            'trend_analysis': 'adhoc.maggieli.top_events_trend_analysis',
            'market_rankings': 'adhoc.maggieli.top_events_market_rankings'
        }
        
        results = {}
        
        for view_name, view_path in views.items():
            query = f"SELECT * FROM {view_path} ORDER BY recent_gms_rank"
            try:
                df = self.execute_query(query)
                results[view_name] = df
                print(f"✅ Loaded {view_name}: {len(df)} rows")
            except Exception as e:
                print(f"❌ Failed to load {view_name}: {e}")
                results[view_name] = pd.DataFrame()
        
        return results
    
    def validate_views_exist(self) -> Dict[str, bool]:
        """Check if all required views exist"""
        views_to_check = [
            'adhoc.maggieli.top_events_last_7_days',
            'adhoc.maggieli.top_events_historical_context', 
            'adhoc.maggieli.top_events_trend_analysis',
            'adhoc.maggieli.top_events_market_rankings'
        ]
        
        results = {}
        
        for view in views_to_check:
            try:
                query = f"SELECT COUNT(*) as row_count FROM {view}"
                result = self.execute_query(query)
                results[view] = True
                print(f"✅ {view}: {result.iloc[0]['ROW_COUNT']} rows")
            except Exception as e:
                results[view] = False
                print(f"❌ {view}: Not accessible - {e}")
        
        return results
    
    def get_sample_data(self, view_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample data from a specific view"""
        query = f"SELECT * FROM {view_name} LIMIT {limit}"
        return self.execute_query(query)
    
    def close_connection(self):
        """Close the Snowflake connection"""
        if self.conn and self.is_local:
            # Only close connector connections, not Snowpark sessions
            self.conn.close()
            print("🔌 Snowflake connection closed")
        elif not self.is_local:
            print("🔌 Snowpark session remains active")


class DataProcessor:
    """Enhanced data processing for social content pipeline"""
    
    def __init__(self):
        self.snowflake = SnowflakeConnector()
    
    def process_event_data(self, raw_data: Dict[str, pd.DataFrame]) -> List[Dict]:
        """Process raw dataframes into structured event objects"""
        if not raw_data.get('base_events') or raw_data['base_events'].empty:
            print("❌ No base events data to process")
            return []
        
        base_df = raw_data['base_events']
        hist_df = raw_data.get('historical_context', pd.DataFrame())
        trend_df = raw_data.get('trend_analysis', pd.DataFrame())
        market_df = raw_data.get('market_rankings', pd.DataFrame())
        
        processed_events = []
        
        for _, row in base_df.iterrows():
            event_id = row['EVENT_ID']
            
            # Find matching rows in other dataframes
            hist_match = self._find_matching_row(hist_df, event_id)
            trend_match = self._find_matching_row(trend_df, event_id)
            market_match = self._find_matching_row(market_df, event_id)
            
            # Build comprehensive event object
            event_obj = self._build_event_object(row, hist_match, trend_match, market_match)
            processed_events.append(event_obj)
        
        return processed_events
    
    def _find_matching_row(self, df: pd.DataFrame, event_id) -> Optional[pd.Series]:
        """Find matching row by event_id"""
        if df.empty:
            return None
        
        matches = df[df['EVENT_ID'] == event_id]
        return matches.iloc[0] if len(matches) > 0 else None
    
    def _build_event_object(self, base_row: pd.Series, hist_row: Optional[pd.Series], 
                           trend_row: Optional[pd.Series], market_row: Optional[pd.Series]) -> Dict:
        """Build comprehensive event object from all data sources"""
        
        # Helper function for safe value extraction with None handling
        def safe_get(row, column, default=None, convert_func=None):
            if row is None:
                return default
            value = row.get(column, default)
            # Handle explicit None values
            if value is None:
                return default
            if convert_func and value is not None:
                try:
                    return convert_func(value)
                except (ValueError, TypeError):
                    return default
            return value
        
        # Special function for artist name handling
        def get_artist_name(base_row):
            classified = base_row.get('CLASSIFIED_ARTIST_NAME')
            category = base_row.get('EVENT_CATEGORY_NAME', 'Unknown')
            
            # Handle None, empty string, or 'None' string
            if classified is None or classified == 'None' or str(classified).strip() == '':
                return str(category)
            return str(classified)
        
        return {
            # Basic event information
            'event_id': str(base_row['EVENT_ID']),
            'event_name': str(base_row['EVENT_NAME']),
            'artist_name': str(base_row.get('EVENT_CATEGORY_NAME', 'Unknown')),
            'classified_artist_name': get_artist_name(base_row),
            'genre': str(base_row['EVENT_PARENT_CATEGORY_NAME']),
            'subgenre': str(base_row.get('SUBGENRE', '')),
            'venue_city': str(base_row['VENUE_CITY']),
            'venue_country': str(base_row['VENUE_COUNTRY_NAME']),
            'event_date': str(base_row['EVENT_DATE']),
            'rank': int(base_row['RECENT_GMS_RANK']),
            
            # Performance metrics
            'total_gms': safe_get(base_row, 'TOTAL_GMS', 0, float),
            'recent_7d_gms': safe_get(base_row, 'RECENT_7D_GMS', 0, float),
            'total_tickets': safe_get(base_row, 'TOTAL_TICKETS_SOLD', 0, int),
            'avg_ticket_cost': safe_get(base_row, 'AVG_TICKET_COST', 0, float),
            'gms_per_ticket': safe_get(base_row, 'GMS_PER_TICKET', 0, float),
            'international_pct': safe_get(base_row, 'INTERNATIONAL_GMS_PCT', 0, float) * 100,
            
            # Career context
            'career_context': {
                'vs_career_avg_multiple': safe_get(hist_row, 'VS_CAREER_AVG_MULTIPLE', 1, float),
                'vs_career_best_ratio': safe_get(hist_row, 'VS_CAREER_BEST_RATIO', 0, float),
                'career_total_events': safe_get(hist_row, 'CAREER_TOTAL_EVENTS', 0, int),
                'career_first_year': safe_get(hist_row, 'CAREER_FIRST_YEAR', 0, int),
                'career_total_gms': safe_get(hist_row, 'CAREER_TOTAL_GMS', 0, float)
            },
            
            # Tour context
            'tour_context': {
                'tour_name': safe_get(hist_row, 'TOUR_NAME', ''),
                'vs_tour_avg_multiple': safe_get(hist_row, 'VS_TOUR_AVG_MULTIPLE', 1, float),
                'tour_total_events': safe_get(hist_row, 'TOUR_TOTAL_EVENTS', 0, int),
                'tour_total_gms': safe_get(hist_row, 'TOUR_TOTAL_GMS', 0, float)
            },
            
            # Genre context
            'genre_context': {
                'vs_genre_avg_multiple': safe_get(hist_row, 'VS_GENRE_AVG_MULTIPLE', 1, float),
                'genre_percentile_bucket': safe_get(hist_row, 'GENRE_PERCENTILE_BUCKET', 'Unknown'),
                'vs_ytd_avg_multiple': safe_get(hist_row, 'VS_YTD_AVG_MULTIPLE', 1, float)
            },
            
            # Trend insights
            'trend_insights': {
                'gms_multiple': safe_get(trend_row, 'GMS_MULTIPLE', 1, float),
                'is_gms_spike': safe_get(trend_row, 'IS_GMS_SPIKE', False, bool),
                'performance_category': safe_get(trend_row, 'PERFORMANCE_CATEGORY', 'Normal'),
                'price_appreciation_pct': safe_get(trend_row, 'PRICE_APPRECIATION_PCT', 0, float) * 100
            },
            
            # Geographic insights
            'geographic_insights': {
                'top_buyer_countries': [
                    {
                        'country': safe_get(trend_row, f'TOP_BUYER_COUNTRY_{i}', ''),
                        'percentage': safe_get(trend_row, f'TOP_BUYER_COUNTRY_{i}_PCT', 0, float) * 100
                    }
                    for i in range(1, 4)
                    if safe_get(trend_row, f'TOP_BUYER_COUNTRY_{i}')
                ]
            },
            
            # Market positioning
            'market_position': {
                'ytd_overall_rank': safe_get(market_row, 'YTD_OVERALL_RANK', 999, int),
                'ytd_genre_rank': safe_get(market_row, 'YTD_GENRE_RANK', 999, int),
                'ytd_overall_tier': safe_get(market_row, 'YTD_OVERALL_TIER', 'Unknown'),
                'ytd_genre_tier': safe_get(market_row, 'YTD_GENRE_TIER', 'Unknown'),
                'last_7d_market_share_pct': safe_get(market_row, 'LAST_7D_MARKET_SHARE_PCT', 0, float) * 100,
                'premium_multiple': safe_get(market_row, 'PREMIUM_MULTIPLE', 1, float)
            },
            
            # Data quality indicators
            'data_completeness': {
                'has_historical_context': hist_row is not None,
                'has_trend_analysis': trend_row is not None,
                'has_market_positioning': market_row is not None,
                'completeness_score': sum([
                    hist_row is not None,
                    trend_row is not None,
                    market_row is not None
                ]) / 3
            }
        }
    
    def validate_data_quality(self, events: List[Dict]) -> Dict:
        """Validate the quality of processed event data"""
        if not events:
            return {'status': 'error', 'message': 'No events to validate'}
        
        total_events = len(events)
        complete_data_events = sum(1 for e in events if e['data_completeness']['completeness_score'] == 1.0)
        avg_completeness = sum(e['data_completeness']['completeness_score'] for e in events) / total_events
        
        # Check for required fields
        required_fields = ['event_id', 'classified_artist_name', 'total_gms', 'recent_7d_gms']
        missing_required = sum(1 for e in events if any(not e.get(field) for field in required_fields))
        
        return {
            'status': 'success',
            'total_events': total_events,
            'complete_data_events': complete_data_events,
            'average_completeness_score': avg_completeness,
            'events_missing_required_fields': missing_required,
            'data_quality_score': (avg_completeness * 0.7) + ((total_events - missing_required) / total_events * 0.3)
        }


def main():
    """Test the data processing functionality"""
    processor = DataProcessor()
    
    # Test connection
    if not processor.snowflake.test_connection():
        print("❌ Cannot proceed without Snowflake connection")
        return
    
    # Validate views exist
    view_status = processor.snowflake.validate_views_exist()
    if not all(view_status.values()):
        print("⚠️  Some views are not accessible")
        return
    
    # Get sample data
    print("\n📊 Getting sample data...")
    raw_data = processor.snowflake.get_top_events_data()
    
    # Process data
    print("\n🔄 Processing event data...")
    events = processor.process_event_data(raw_data)
    
    # Validate quality
    quality_report = processor.validate_data_quality(events)
    print(f"\n📈 Data Quality Report:")
    print(f"  Total Events: {quality_report['total_events']}")
    print(f"  Complete Data: {quality_report['complete_data_events']}")
    print(f"  Avg Completeness: {quality_report['average_completeness_score']:.2%}")
    print(f"  Overall Quality Score: {quality_report['data_quality_score']:.2%}")
    
    processor.snowflake.close_connection()


if __name__ == "__main__":
    main()