import streamlit as st
import pandas as pd
import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to path for imports
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Import social content pipeline components
from social_content_generator import SocialContentPipeline
from data_processing import SnowflakeConnector
from ai_contextualizer import ContentGenerator
from batch_processor import BatchProcessor

# Configure Streamlit page
st.set_page_config(
    page_title="Social Content Generator",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

def load_custom_css():
    """Load custom CSS styling"""
    st.markdown("""
    <style>
        .main-header {
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            margin-bottom: 2rem;
        }
        
        .content-card {
            background: white;
            padding: 1.5rem;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            margin-bottom: 1rem;
            border-left: 4px solid #667eea;
        }
        
        .success-message {
            background: rgba(40, 167, 69, 0.1);
            padding: 1rem;
            border-radius: 8px;
            border-left: 3px solid #28a745;
            margin-top: 1rem;
        }
        
        .warning-message {
            background: rgba(255, 193, 7, 0.1);
            padding: 1rem;
            border-radius: 8px;
            border-left: 3px solid #ffc107;
            margin-top: 1rem;
        }
        
        .error-message {
            background: rgba(220, 53, 69, 0.1);
            padding: 1rem;
            border-radius: 8px;
            border-left: 3px solid #dc3545;
            margin-top: 1rem;
        }
        
        .metric-card {
            background: #f8f9ff;
            padding: 1rem;
            border-radius: 8px;
            text-align: center;
            border: 1px solid #e0e7ff;
        }
    </style>
    """, unsafe_allow_html=True)

class SocialContentApp:
    def __init__(self):
        """Initialize the Social Content Generator app"""
        self.pipeline = None
        self.snowflake_connector = None
        self.initialize_session_state()
        self.initialize_components()
    
    def initialize_session_state(self):
        """Initialize session state variables for data persistence"""
        # Core data
        if 'data_loaded' not in st.session_state:
            st.session_state.data_loaded = False
        if 'snowflake_data' not in st.session_state:
            st.session_state.snowflake_data = {}
        if 'structured_events' not in st.session_state:
            st.session_state.structured_events = []
        if 'selected_events' not in st.session_state:
            st.session_state.selected_events = []
        
        # Content generation
        if 'generated_content' not in st.session_state:
            st.session_state.generated_content = []
        if 'custom_prompts' not in st.session_state:
            st.session_state.custom_prompts = {}
        if 'content_generated' not in st.session_state:
            st.session_state.content_generated = False
        
        # UI state
        if 'current_step' not in st.session_state:
            st.session_state.current_step = 'load_data'
        if 'last_error' not in st.session_state:
            st.session_state.last_error = None
        if 'export_history' not in st.session_state:
            st.session_state.export_history = []
    
    def initialize_components(self):
        """Initialize pipeline components with proper error handling"""
        try:
            with st.spinner("🔧 Initializing components..."):
                self.pipeline = SocialContentPipeline()
                self.snowflake_connector = SnowflakeConnector()
            st.session_state.last_error = None
        except ImportError as e:
            error_msg = f"Missing dependencies: {str(e)}. Please check your environment setup."
            st.error(f"❌ {error_msg}")
            st.session_state.last_error = error_msg
        except Exception as e:
            error_msg = f"Failed to initialize components: {str(e)}"
            st.error(f"❌ {error_msg}")
            st.session_state.last_error = error_msg
    
    def reset_app_state(self):
        """Reset all session state to start over"""
        keys_to_clear = [
            'data_loaded', 'snowflake_data', 'structured_events', 'selected_events',
            'generated_content', 'custom_prompts', 'content_generated', 'current_step',
            'last_error', 'export_history'
        ]
        
        for key in keys_to_clear:
            if key in st.session_state:
                del st.session_state[key]
        
        self.initialize_session_state()
        st.success("✅ Application reset successfully! All data cleared.")
        st.experimental_rerun()
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def load_snowflake_data(_self):
        """Load and cache data from all 4 Snowflake views"""
        try:
            # Connect to Snowflake
            if not _self.snowflake_connector.connect():
                raise Exception("Failed to connect to Snowflake")
            
            # Query all views
            dataframes = _self.pipeline.query_top_events_views()
            
            # Close connection
            _self.snowflake_connector.close_connection()
            
            return dataframes, None
            
        except Exception as e:
            return None, str(e)
    
    def render_data_loading_section(self):
        """Render comprehensive data loading section with caching and summaries"""
        st.subheader("📊 Data Loading & Summary")
        
        # Show helpful information
        with st.expander("📝 About Data Loading", expanded=False):
            st.markdown("""
            This section loads event data from 4 Snowflake views:
            - **Base Events**: Core event information and metrics
            - **Historical Context**: Career and tour comparisons
            - **Trend Analysis**: Performance trends and spikes
            - **Market Rankings**: Genre and overall market positioning
            
            Data is cached for 5 minutes to improve performance.
            """)
        
        # Load data button with auto-refresh option
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        
        with col1:
            load_data = st.button("🔄 Load Latest Data", type="primary", 
                                help="Fetch fresh data from Snowflake")
        
        with col2:
            if st.button("🗑️ Clear Cache", help="Clear cached data to force refresh"):
                try:
                    st.cache_data.clear()
                    st.success("✅ Cache cleared successfully!")
                except Exception as e:
                    st.error(f"❌ Failed to clear cache: {str(e)}")
        
        with col3:
            auto_refresh = st.checkbox("Auto-refresh", help="Automatically load data on page load")
        
        with col4:
            if st.button("🔄 Reset App", help="Clear all data and start over"):
                self.reset_app_state()
                return None
        
        # Load data if button clicked, auto-refresh enabled, or no data exists
        should_load = (load_data or auto_refresh or not st.session_state.data_loaded)
        
        if should_load:
            try:
                with st.spinner("🔍 Connecting to Snowflake and querying views..."):
                    dataframes, error = self.load_snowflake_data()
                    
                    if error:
                        st.error(f"❌ Failed to load data: {error}")
                        st.session_state.last_error = error
                        st.session_state.data_loaded = False
                        return None
                    
                    if not dataframes or dataframes.get('base_events') is None or dataframes['base_events'].empty:
                        error_msg = "No data returned from Snowflake views. Please check your connection and view permissions."
                        st.error(f"❌ {error_msg}")
                        st.session_state.last_error = error_msg
                        st.session_state.data_loaded = False
                        return None
                    
                    # Store in session state
                    st.session_state.snowflake_data = dataframes
                    st.session_state.data_loaded = True
                    st.session_state.last_error = None
                    st.session_state.current_step = 'select_events'
                    
                    # Structure the events
                    with st.spinner("🔗 Structuring event data..."):
                        structured_events = self.pipeline.structure_event_data(dataframes)
                        st.session_state.structured_events = structured_events
                        
            except Exception as e:
                error_msg = f"Unexpected error during data loading: {str(e)}"
                st.error(f"❌ {error_msg}")
                st.session_state.last_error = error_msg
                st.session_state.data_loaded = False
                return None
        
        # Display loaded data summary if available
        if st.session_state.data_loaded and st.session_state.snowflake_data:
            dataframes = st.session_state.snowflake_data
            
            # Success message
            st.success(f"✅ Successfully loaded {len(st.session_state.structured_events)} events from Snowflake!")
            
            # Summary metrics
            try:
                self.render_data_summary(dataframes)
            except Exception as e:
                st.warning(f"⚠️ Could not display data summary: {str(e)}")
            
            # Expandable dataframes
            try:
                self.render_expandable_dataframes(dataframes)
            except Exception as e:
                st.warning(f"⚠️ Could not display detailed data: {str(e)}")
            
            return dataframes
        
        else:
            if st.session_state.last_error:
                st.error(f"❌ Last error: {st.session_state.last_error}")
            st.info("🔍 Click 'Load Latest Data' to fetch events from Snowflake")
            return None
    
    def render_data_summary(self, dataframes):
        """Render summary statistics of loaded data"""
        base_events = dataframes.get('base_events', pd.DataFrame())
        
        if base_events.empty:
            st.warning("⚠️ No base events data available")
            return
        
        st.markdown("### 📈 Data Summary")
        
        # Summary metrics in columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_events = len(base_events)
            st.metric("Total Events", total_events)
        
        with col2:
            # Get unique artists/performers
            unique_artists = base_events['EVENT_CATEGORY_NAME'].nunique()
            st.metric("Unique Artists", unique_artists)
        
        with col3:
            # Top genre
            if 'EVENT_PARENT_CATEGORY_NAME' in base_events.columns:
                top_genre = base_events['EVENT_PARENT_CATEGORY_NAME'].mode().iloc[0] if len(base_events) > 0 else 'N/A'
                st.metric("Top Genre", top_genre)
            else:
                st.metric("Top Genre", "N/A")
        
        with col4:
            # Average rank
            if 'RECENT_GMS_RANK' in base_events.columns:
                try:
                    # Convert to numeric and calculate mean
                    rank_numeric = pd.to_numeric(base_events['RECENT_GMS_RANK'], errors='coerce')
                    avg_rank = rank_numeric.mean()
                    if pd.notna(avg_rank):
                        st.metric("Avg Rank", f"#{avg_rank:.1f}")
                    else:
                        st.metric("Avg Rank", "N/A")
                except:
                    st.metric("Avg Rank", "N/A")
            else:
                st.metric("Avg Rank", "N/A")
        
        # Top artists/events
        st.markdown("### 🏆 Top Performers")
        
        if 'RECENT_7D_GMS' in base_events.columns:
            try:
                # Convert to numeric, handling any non-numeric values
                base_events_clean = base_events.copy()
                base_events_clean['RECENT_7D_GMS'] = pd.to_numeric(base_events_clean['RECENT_7D_GMS'], errors='coerce')
                base_events_clean['RECENT_GMS_RANK'] = pd.to_numeric(base_events_clean['RECENT_GMS_RANK'], errors='coerce')
                
                # Remove rows with null GMS values
                base_events_clean = base_events_clean.dropna(subset=['RECENT_7D_GMS'])
                
                if len(base_events_clean) > 0:
                    # Sort by recent GMS and show top 5
                    top_events = base_events_clean.nlargest(5, 'RECENT_7D_GMS')[
                        ['EVENT_CATEGORY_NAME', 'EVENT_NAME', 'VENUE_CITY', 'RECENT_7D_GMS', 'RECENT_GMS_RANK']
                    ].copy()
                    
                    # Format GMS values
                    top_events['RECENT_7D_GMS'] = top_events['RECENT_7D_GMS'].apply(lambda x: f"${x:,.0f}")
                    top_events['RECENT_GMS_RANK'] = top_events['RECENT_GMS_RANK'].apply(lambda x: f"#{int(x)}" if pd.notna(x) else "N/A")
                    
                    # Cleaner column names
                    top_events.columns = ['Artist/Team', 'Event', 'City', 'Recent 7d GMS', 'Rank']
                    
                    st.dataframe(top_events, use_container_width=True, hide_index=True)
                else:
                    st.warning("⚠️ No valid GMS data available for top performers")
                    
            except Exception as e:
                st.warning(f"⚠️ Could not display top performers: {str(e)}")
                # Show basic info without sorting
                if len(base_events) > 0:
                    basic_events = base_events.head(5)[
                        ['EVENT_CATEGORY_NAME', 'EVENT_NAME', 'VENUE_CITY']
                    ].copy()
                    basic_events.columns = ['Artist/Team', 'Event', 'City']
                    st.dataframe(basic_events, use_container_width=True, hide_index=True)
        
        # Genre breakdown
        if 'EVENT_PARENT_CATEGORY_NAME' in base_events.columns:
            st.markdown("### 🎭 Genre Distribution")
            
            genre_counts = base_events['EVENT_PARENT_CATEGORY_NAME'].value_counts()
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Create a simple bar chart
                st.bar_chart(genre_counts)
            
            with col2:
                # Show as table
                genre_df = pd.DataFrame({
                    'Genre': genre_counts.index,
                    'Count': genre_counts.values,
                    'Percentage': (genre_counts.values / len(base_events) * 100).round(1)
                })
                st.dataframe(genre_df, use_container_width=True, hide_index=True)
    
    def render_expandable_dataframes(self, dataframes):
        """Render expandable sections for each dataframe"""
        st.markdown("### 📋 Raw Data Tables")
        
        # View status summary
        view_status = []
        for view_name, df in dataframes.items():
            status = "✅ Loaded" if not df.empty else "⚠️ Empty"
            count = len(df) if not df.empty else 0
            view_status.append({
                'View': view_name.replace('_', ' ').title(),
                'Status': status,
                'Row Count': count
            })
        
        # Show view status table
        status_df = pd.DataFrame(view_status)
        st.dataframe(status_df, use_container_width=True, hide_index=True)
        
        st.markdown("---")
        
        # Expandable sections for each view
        for view_name, df in dataframes.items():
            if not df.empty:
                with st.expander(f"📊 {view_name.replace('_', ' ').title()} ({len(df)} rows)", expanded=False):
                    
                    # Show column info
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        st.markdown("**Columns:**")
                        for col in df.columns:
                            st.text(f"• {col}")
                    
                    with col2:
                        st.markdown("**Data Types:**")
                        for col, dtype in df.dtypes.items():
                            st.text(f"• {col}: {dtype}")
                    
                    st.markdown("**Sample Data:**")
                    
                    # Show sample data with option to view more
                    sample_size = st.selectbox(
                        f"Rows to display for {view_name}",
                        options=[5, 10, 25, 50, len(df)],
                        index=0,
                        key=f"sample_size_{view_name}"
                    )
                    
                    display_df = df.head(sample_size) if sample_size < len(df) else df
                    st.dataframe(display_df, use_container_width=True)
                    
                    # Download option
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label=f"📥 Download {view_name} as CSV",
                        data=csv,
                        file_name=f"{view_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        key=f"download_{view_name}"
                    )
            else:
                with st.expander(f"⚠️ {view_name.replace('_', ' ').title()} (Empty)", expanded=False):
                    st.warning(f"No data available in {view_name} view")
                    st.info("This could indicate:")
                    st.markdown("""
                    - View permissions issues
                    - No data matching current criteria
                    - View definition problems
                    - Connection timeout
                    """)
    
    def get_cached_data(self):
        """Get cached data if available"""
        return st.session_state.get('loaded_data', None)
    
    def render_event_selection_interface(self, events_data, max_events):
        """Render event selection interface with metrics and content preview"""
        st.subheader("🎯 Event Selection")
        
        # Help information
        with st.expander("📝 Event Selection Guide", expanded=False):
            st.markdown("""
            **How to select events:**
            - **Select All**: Automatically selects all available events (up to limit)
            - **Multi-Select**: Choose multiple specific events from the dropdown
            - **Single Select**: Focus on one event for detailed content generation
            
            **Content angles are automatically identified** based on:
            - Performance spikes vs career average
            - International buyer percentage  
            - Genre rankings and market position
            - Tour performance comparisons
            """)
        
        if not events_data:
            st.warning("⚠️ No events available. Please load data first.")
            return []
        
        try:
            # Create event options for selection
            event_options = []
            event_lookup = {}
            
            for event in events_data:
                try:
                    # Use event_category_name if classified_artist_name is null/None
                    artist_name = event.get('classified_artist_name', 'Unknown')
                    if artist_name in ['Unknown', 'None', None, 'nan', '']:
                        artist_name = event.get('artist_name', 'Unknown')
                    
                    # Create display label with error handling
                    event_name = event.get('event_name', 'Unknown Event')
                    venue_city = event.get('venue_city', 'Unknown City')
                    display_label = f"{artist_name} - {event_name} ({venue_city})"
                    
                    event_options.append(display_label)
                    event_lookup[display_label] = event
                    
                except Exception as e:
                    st.warning(f"⚠️ Skipping malformed event data: {str(e)}")
                    continue
            
            if not event_options:
                st.error("❌ No valid events found in the data.")
                return []
            
            # Event selection controls
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Selection type toggle
                selection_type = st.radio(
                    "Selection Mode",
                    ["Select All", "Multi-Select", "Single Select"],
                    horizontal=True,
                    help="Choose how to select events for content generation"
                )
            
            with col2:
                st.metric("Available Events", len(event_options))
            
            selected_event_labels = []
            
            if selection_type == "Select All":
                selected_event_labels = event_options[:max_events]
                st.success(f"✅ All {len(selected_event_labels)} events selected (limited to {max_events})")
                
            elif selection_type == "Multi-Select":
                # Use session state to preserve selection
                if 'selected_event_labels' not in st.session_state:
                    st.session_state.selected_event_labels = event_options[:min(3, len(event_options))]
                
                selected_event_labels = st.multiselect(
                    "Choose events for content generation",
                    options=event_options,
                    default=st.session_state.selected_event_labels if st.session_state.selected_event_labels else [],
                    help=f"Select up to {max_events} events",
                    key="event_multiselect"
                )
                
                # Update session state
                st.session_state.selected_event_labels = selected_event_labels
                
                # Limit selection with user feedback
                if len(selected_event_labels) > max_events:
                    st.warning(f"⚠️ Too many events selected ({len(selected_event_labels)}). Limiting to first {max_events}.")
                    selected_event_labels = selected_event_labels[:max_events]
                    st.session_state.selected_event_labels = selected_event_labels
                elif len(selected_event_labels) == 0:
                    st.info("📝 Select at least one event to continue.")
                else:
                    st.success(f"✅ {len(selected_event_labels)} events selected")
                    
            else:  # Single Select
                selected_label = st.selectbox(
                    "Choose one event for content generation",
                    options=event_options,
                    help="Select a single event to focus content generation",
                    key="event_selectbox"
                )
                selected_event_labels = [selected_label] if selected_label else []
                
                if selected_event_labels:
                    st.success("✅ Event selected for focused content generation")
            
            # Get selected events data with error handling
            selected_events = []
            for label in selected_event_labels:
                try:
                    event = event_lookup[label]
                    selected_events.append(event)
                except KeyError:
                    st.warning(f"⚠️ Could not find event data for: {label}")
                    continue
            
            # Store in session state
            st.session_state.selected_events = selected_events
            
        except Exception as e:
            st.error(f"❌ Error in event selection: {str(e)}")
            return []
        
        if selected_events:
            # Display selected events with metrics and content preview
            st.markdown(f"#### 📊 Selected Events ({len(selected_events)})")
            
            for i, event in enumerate(selected_events, 1):
                with st.expander(f"🎵 {i}. {event.get('classified_artist_name', event.get('artist_name', 'Unknown'))} - {event['event_name']}", expanded=False):
                    
                    # Event metrics
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("Rank", f"#{event.get('rank', 'N/A')}")
                    
                    with col2:
                        st.metric("Genre", event.get('genre', 'N/A'))
                    
                    with col3:
                        recent_gms = event.get('recent_7d_gms', 0)
                        if recent_gms:
                            st.metric("Recent 7d GMS", f"${recent_gms:,.0f}")
                        else:
                            st.metric("Recent 7d GMS", "N/A")
                    
                    with col4:
                        career_multiple = event.get('career_context', {}).get('vs_career_avg_multiple', 0)
                        if career_multiple:
                            st.metric("vs Career Avg", f"{career_multiple:.1f}x")
                        else:
                            st.metric("vs Career Avg", "N/A")
                    
                    # Content angles identification
                    st.markdown("**🎯 Content Angles:**")
                    try:
                        content_angles = self.pipeline.identify_content_angles(event)
                        if content_angles:
                            for angle in content_angles:
                                # Format angle name
                                formatted_angle = angle.replace('_', ' ').title()
                                
                                # Add emoji based on angle type
                                angle_emoji = self.get_angle_emoji(angle)
                                st.markdown(f"• {angle_emoji} **{formatted_angle}**")
                        else:
                            st.markdown("• 📈 **Trending Event** (default)")
                    except Exception as e:
                        st.warning(f"Could not identify content angles: {str(e)}")
                    
                    # Performance insights
                    st.markdown("**📈 Key Insights:**")
                    insights = []
                    
                    # Rank insight
                    rank = event.get('rank')
                    if rank and rank <= 5:
                        insights.append(f"🏆 Top {rank} performer")
                    
                    # Career performance
                    career_multiple = event.get('career_context', {}).get('vs_career_avg_multiple', 0)
                    if career_multiple >= 3:
                        insights.append(f"🚀 {career_multiple:.1f}x above career average")
                    elif career_multiple >= 2:
                        insights.append(f"📈 {career_multiple:.1f}x above career average")
                    
                    # International appeal
                    intl_pct = event.get('international_pct', 0)
                    if intl_pct > 30:
                        insights.append(f"🌍 {intl_pct:.0f}% international buyers")
                    
                    # Genre positioning
                    genre_rank = event.get('market_position', {}).get('ytd_genre_rank')
                    if genre_rank and genre_rank <= 10:
                        insights.append(f"🎭 #{genre_rank} in {event.get('genre', 'genre')}")
                    
                    if insights:
                        for insight in insights:
                            st.markdown(f"• {insight}")
                    else:
                        st.markdown("• 📊 Strong performance metrics")
                    
                    # Content preview
                    st.markdown("**📝 Content Preview:**")
                    try:
                        preview_angles = content_angles[:2] if content_angles else ['trending_event']
                        for angle in preview_angles:
                            formatted_angle = angle.replace('_', ' ').title()
                            preview_text = self.generate_content_preview(event, angle)
                            st.markdown(f"*{formatted_angle}:* {preview_text}")
                    except Exception as e:
                        st.markdown("*Preview will be generated during content creation*")
        
        return selected_events
    
    def get_angle_emoji(self, angle):
        """Get emoji for content angle"""
        angle_emojis = {
            'major_spike': '🚀',
            'significant_spike': '📈', 
            'notable_performance': '⭐',
            'international_phenomenon': '🌍',
            'international_appeal': '🌎',
            'genre_leader': '👑',
            'top_performer': '🏆',
            'pricing_surge': '💰',
            'demand_indicator': '📊',
            'tour_standout': '🔥',
            'top_performance': '⭐',
            'trending_event': '📈'
        }
        return angle_emojis.get(angle, '🎵')
    
    def generate_content_preview(self, event, angle):
        """Generate a preview of content for the given event and angle"""
        artist_name = event.get('classified_artist_name', event.get('artist_name', 'Artist'))
        if artist_name in ['Unknown', 'None', None, 'nan', '']:
            artist_name = event.get('artist_name', 'Artist')
        
        venue_city = event.get('venue_city', 'City')
        recent_gms = event.get('recent_7d_gms', 0)
        career_multiple = event.get('career_context', {}).get('vs_career_avg_multiple', 0)
        rank = event.get('rank', 0)
        
        # Generate preview based on angle
        if angle == 'significant_spike' and career_multiple >= 3:
            return f"🔥 {artist_name} is ON FIRE in {venue_city}! Performing at {career_multiple:.1f}x above their career average..."
        elif angle == 'genre_leader' and rank <= 5:
            return f"👑 {artist_name} is CRUSHING it at #{rank} this week! Dominating the charts..."
        elif angle == 'tour_standout' and recent_gms:
            return f"🎪 {artist_name}'s {venue_city} show is breaking records with ${recent_gms:,.0f} in sales..."
        elif angle == 'top_performer':
            return f"⭐ {artist_name} continues their incredible run with another standout performance..."
        elif angle == 'international_appeal':
            return f"🌍 {artist_name} is capturing hearts worldwide with their {venue_city} show..."
        else:
            return f"📈 {artist_name} is trending with their latest performance in {venue_city}..."
    
    def render_prompt_editing_section(self, selected_events):
        """Render prompt editing interface with templates and preview"""
        
        # Template definitions (copied from ai_contextualizer.py)
        self.prompt_templates = {
            'major_spike': {
                'name': '🚀 Major Spike (5x+ Career Average)',
                'template': """Create viral {platform} content about this MASSIVE performance spike. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: Performing {career_multiple:.1f}x above career average - this is HUGE
SUPPORTING DATA: {intl_pct:.0f}% international buyers, #{rank} trending this week
{fandom_context}"""
            },
            'significant_spike': {
                'name': '📈 Significant Spike (3-5x Career Average)', 
                'template': """Create viral {platform} content about this SIGNIFICANT performance spike. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: Performing {career_multiple:.1f}x above career average - this is significant
SUPPORTING DATA: {intl_pct:.0f}% international buyers, #{rank} trending this week
{fandom_context}"""
            },
            'genre_leader': {
                'name': '👑 Genre Leader',
                'template': """Create viral {platform} content celebrating this genre-leading performance. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: #{genre_rank} in {genre} this year, #{overall_rank} overall
SUPPORTING DATA: Genre-leading performance, top tier positioning
{fandom_context}"""
            },
            'tour_standout': {
                'name': '🔥 Tour Standout',
                'template': """Create viral {platform} content about this standout tour performance. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: {tour_multiple:.1f}x above tour average for {tour_name}
SUPPORTING DATA: Standout performance in tour, exceptional demand
{fandom_context}"""
            },
            'international_phenomenon': {
                'name': '🌍 International Phenomenon',
                'template': """Create viral {platform} content about this international phenomenon. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}  
KEY INSIGHT: {intl_pct:.0f}% international buyers - incredible global appeal
SUPPORTING DATA: Worldwide demand, cross-cultural appeal
{fandom_context}"""
            },
            'top_performer': {
                'name': '🏆 Top Performer',
                'template': """Create viral {platform} content about this top-tier performance. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: #{rank} performer this week, consistent excellence
SUPPORTING DATA: Top-tier positioning, strong market performance
{fandom_context}"""
            },
            'trending_event': {
                'name': '📈 Trending Event (Default)',
                'template': """Create viral {platform} content about this trending event. Remember: NO dollar amounts!
EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: Trending #{rank} this week, strong performance
SUPPORTING DATA: Market momentum, fan engagement
{fandom_context}"""
            }
        }
        
        # System prompt
        self.system_prompt = """You are a Gen Z social media expert creating viral content for live events and entertainment. 
Your content should be data-driven but never boring, optimized for discovery, and designed to make people stop scrolling.

CRITICAL RULES:
1. NEVER share actual dollar amounts or GMS numbers - use relative terms like "massive surge" or "top performer"
2. Always provide TWO separate outputs: VISUAL TEXT and CAPTION
3. Write like Gen Z (but not cringe) - authentic, direct, no millennial energy
4. Front-load artist/team names for SEO and discovery

For Instagram/TikTok:
- VISUAL TEXT: Punchy, data-forward, shareable. Think billboard text - immediate impact, no context needed
- CAPTION: Keyword-optimized, artist name first, context for fans, discovery-friendly hashtags
- Make it something fans want to repost to their Stories with their own reaction"""
        
        # Event selection for prompt editing
        if len(selected_events) > 1:
            event_options = []
            for i, event in enumerate(selected_events):
                artist_name = event.get('classified_artist_name', event.get('artist_name', 'Unknown'))
                if artist_name in ['Unknown', 'None', None, 'nan', '']:
                    artist_name = event.get('artist_name', 'Unknown')
                event_options.append(f"{i+1}. {artist_name} - {event['event_name']}")
            
            selected_event_idx = st.selectbox(
                "Choose event to customize prompt for:",
                options=range(len(selected_events)),
                format_func=lambda x: event_options[x],
                help="Select which event to customize the prompt for"
            )
            current_event = selected_events[selected_event_idx]
        else:
            current_event = selected_events[0]
            artist_name = current_event.get('classified_artist_name', current_event.get('artist_name', 'Unknown'))
            if artist_name in ['Unknown', 'None', None, 'nan', '']:
                artist_name = current_event.get('artist_name', 'Unknown')
            st.info(f"📝 Editing prompt for: **{artist_name} - {current_event['event_name']}**")
        
        # Get content angles for this event
        try:
            available_angles = self.pipeline.identify_content_angles(current_event)
            if not available_angles:
                available_angles = ['trending_event']
        except:
            available_angles = ['trending_event']
        
        # Template selection
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Filter templates to show only relevant ones + default
            relevant_templates = {k: v for k, v in self.prompt_templates.items() if k in available_angles}
            if not relevant_templates:
                relevant_templates = {'trending_event': self.prompt_templates['trending_event']}
            
            selected_template = st.selectbox(
                "📋 Choose Template:",
                options=list(relevant_templates.keys()),
                format_func=lambda x: relevant_templates[x]['name'],
                help="Templates are filtered based on event's content angles"
            )
        
        with col2:
            platform = st.selectbox(
                "📱 Platform:",
                options=['instagram', 'tiktok', 'twitter'],
                index=0,
                help="Choose platform for content optimization"
            )
        
        # Show available data placeholders
        st.markdown("#### 📊 Available Data Placeholders")
        
        placeholder_col1, placeholder_col2 = st.columns(2)
        
        with placeholder_col1:
            st.markdown("**Event Data:**")
            st.code("""
{artist} - Artist/performer name
{event_name} - Event name
{location} - City, Country
{rank} - Current ranking
{genre} - Music/event genre
{venue_city} - Venue city
{venue_country} - Venue country
            """)
        
        with placeholder_col2:
            st.markdown("**Performance Metrics:**")
            st.code("""
{career_multiple} - vs career avg (e.g., 3.5)
{intl_pct} - International buyer %
{genre_rank} - Rank in genre
{overall_rank} - Overall rank
{tour_name} - Tour name
{tour_multiple} - vs tour avg
{fandom_context} - Genre-specific context
            """)
        
        # Prompt editing
        st.markdown("#### ✏️ Edit Prompt Template")
        
        # Get current template
        current_template = relevant_templates[selected_template]['template']
        
        # Editable prompt
        edited_prompt = st.text_area(
            "User Prompt Template:",
            value=current_template,
            height=200,
            help="Edit the prompt template. Use {placeholders} for dynamic data insertion."
        )
        
        # System prompt editing
        with st.expander("🔧 Advanced: Edit System Prompt", expanded=False):
            edited_system_prompt = st.text_area(
                "System Prompt:",
                value=self.system_prompt,
                height=300,
                help="Edit the system prompt that provides context to ChatGPT"
            )
        
        if 'edited_system_prompt' not in locals():
            edited_system_prompt = self.system_prompt
        
        # Generate preview data
        st.markdown("#### 👀 Prompt Preview")
        
        # Prepare data for placeholder replacement
        artist_name = current_event.get('classified_artist_name', current_event.get('artist_name', 'Unknown'))
        if artist_name in ['Unknown', 'None', None, 'nan', '']:
            artist_name = current_event.get('artist_name', 'Unknown')
        
        # Genre-specific fandom context
        genre = current_event.get('genre', '').lower()
        fandom_context = ""
        if 'hip hop' in genre or 'rap' in genre:
            fandom_context = "Consider adding hip-hop culture references if relevant"
        elif 'rock' in genre:
            fandom_context = "Consider rock/metal culture references if relevant"
        elif 'country' in genre:
            fandom_context = "Consider country music culture references if relevant"
        elif 'pop' in genre:
            fandom_context = "Consider pop culture references if relevant"
        elif 'sports' in genre:
            fandom_context = "Consider sports culture and team loyalty references if relevant"
        
        preview_data = {
            'artist': artist_name,
            'event_name': current_event.get('event_name', 'Event'),
            'location': f"{current_event.get('venue_city', 'City')}, {current_event.get('venue_country', 'Country')}",
            'rank': current_event.get('rank', 1),
            'genre': current_event.get('genre', 'Music'),
            'venue_city': current_event.get('venue_city', 'City'),
            'venue_country': current_event.get('venue_country', 'Country'),
            'career_multiple': current_event.get('career_context', {}).get('vs_career_avg_multiple', 2.5),
            'intl_pct': current_event.get('international_pct', 15),
            'genre_rank': current_event.get('market_position', {}).get('ytd_genre_rank', 5),
            'overall_rank': current_event.get('market_position', {}).get('ytd_overall_rank', 10),
            'tour_name': current_event.get('tour_context', {}).get('tour_name', 'World Tour'),
            'tour_multiple': current_event.get('tour_context', {}).get('vs_tour_avg_multiple', 1.8),
            'platform': platform,
            'fandom_context': fandom_context
        }
        
        # Generate final prompt preview
        try:
            final_prompt = edited_prompt.format(**preview_data)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**📝 Final User Prompt:**")
                st.code(final_prompt, language="text")
            
            with col2:
                st.markdown("**🤖 System Prompt:**")
                st.code(edited_system_prompt[:500] + "..." if len(edited_system_prompt) > 500 else edited_system_prompt, language="text")
            
            # Store edited prompts in session state for use during generation
            if 'custom_prompts' not in st.session_state:
                st.session_state['custom_prompts'] = {}
            
            st.session_state['custom_prompts'] = {
                'user_prompt_template': edited_prompt,
                'system_prompt': edited_system_prompt,
                'selected_template': selected_template,
                'platform': platform
            }
            
            st.success("✅ Prompt customization saved! These will be used during content generation.")
            
        except KeyError as e:
            st.error(f"❌ Invalid placeholder in prompt: {e}")
            st.info("💡 Check that all placeholders match the available data fields above")
        except Exception as e:
            st.error(f"❌ Error generating prompt preview: {str(e)}")
    
    def run_enhanced_content_generation(self, selected_events):
        """Run enhanced content generation with real-time progress and error handling"""
        import time
        from ai_contextualizer import ContentGenerator
        
        
        # Initialize content generator
        content_generator = ContentGenerator()
        
        # Check for custom prompts
        custom_prompts = st.session_state.get('custom_prompts', {})
        
        # Overall progress tracking
        main_progress = st.progress(0)
        main_status = st.empty()
        
        # Real-time content display
        content_container = st.container()
        
        try:
            # Step 1: Prepare events
            main_status.text("🔗 Preparing events for content generation...")
            main_progress.progress(0.1)
            
            if not selected_events:
                st.error("❌ No events selected")
                return
            
            # Calculate total content pieces (events × angles)
            total_pieces = 0
            event_content_map = {}
            
            for event in selected_events:
                try:
                    angles = self.pipeline.identify_content_angles(event)
                    if not angles:
                        angles = ['trending_event']
                    event_content_map[event['event_id']] = angles
                    total_pieces += len(angles)
                except Exception as e:
                    st.warning(f"⚠️ Could not identify angles for {event.get('artist_name', 'Unknown')}: {str(e)}")
                    event_content_map[event['event_id']] = ['trending_event']
                    total_pieces += 1
            
            main_status.text(f"📊 Will generate {total_pieces} pieces of content...")
            main_progress.progress(0.2)
            
            # Step 2: Generate content with real-time updates
            main_status.text("✍️ Generating social media content...")
            
            all_content = []
            current_piece = 0
            
            # Real-time content display sections
            with content_container:
                st.markdown("### 🎨 Generated Content (Real-time)")
                content_placeholder = st.empty()
                
                content_display = {}
                
                for event in selected_events:
                    event_id = event['event_id']
                    artist_name = event.get('classified_artist_name', event.get('artist_name', 'Unknown'))
                    if artist_name in ['Unknown', 'None', None, 'nan', '']:
                        artist_name = event.get('artist_name', 'Unknown')
                    
                    angles = event_content_map.get(event_id, ['trending_event'])
                    
                    # Create section for this event
                    event_key = f"{artist_name} - {event['event_name']}"
                    content_display[event_key] = {}
                    
                    # Generate content for each angle
                    for angle in angles:
                        current_piece += 1
                        piece_progress = current_piece / total_pieces
                        
                        # Update main progress
                        main_progress.progress(0.2 + (piece_progress * 0.6))
                        main_status.text(f"✍️ Generating {angle.replace('_', ' ').title()} content for {artist_name}... ({current_piece}/{total_pieces})")
                        
                        # Generate individual piece with error handling
                        try:
                            content_item = self.generate_single_content_piece(
                                content_generator, event, angle, custom_prompts
                            )
                            
                            if content_item:
                                all_content.append(content_item)
                                content_display[event_key][angle] = {
                                    'status': '✅ Generated',
                                    'content': content_item,
                                    'error': None
                                }
                            else:
                                content_display[event_key][angle] = {
                                    'status': '❌ Failed',
                                    'content': None,
                                    'error': 'Generation failed'
                                }
                        
                        except Exception as e:
                            error_msg = str(e)
                            content_display[event_key][angle] = {
                                'status': '❌ Error',
                                'content': None,
                                'error': error_msg
                            }
                            
                            # Handle specific errors
                            if "rate_limit" in error_msg.lower():
                                st.warning("⏱️ Rate limit detected. Waiting 10 seconds...")
                                time.sleep(10)
                            elif "timeout" in error_msg.lower():
                                st.warning("⏱️ Timeout detected. Retrying in 5 seconds...")
                                time.sleep(5)
                        
                        # Update real-time display
                        self.update_content_display(content_placeholder, content_display)
                        
                        # Small delay to prevent rate limiting
                        time.sleep(1)
            
            # Step 3: Process and display results
            main_status.text("📋 Organizing generated content...")
            main_progress.progress(0.9)
            
            if all_content:
                # Complete
                main_progress.progress(1.0)
                main_status.text("✅ Content generation completed!")
                
                # Success summary
                st.success(f"🎉 Generated {len(all_content)} pieces of content!")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Content Pieces", len(all_content))
                
                with col2:
                    unique_events = len(set(item['event_id'] for item in all_content))
                    st.metric("Events Covered", unique_events)
                
                with col3:
                    success_rate = len(all_content) / total_pieces * 100
                    st.metric("Success Rate", f"{success_rate:.1f}%")
                
                # Store in session state for display
                st.session_state['generated_content'] = all_content
                st.session_state['content_generated'] = True
                st.session_state['content_metadata'] = {
                    'generated_at': datetime.now().isoformat(),
                    'total_pieces': len(all_content),
                    'unique_events': unique_events,
                    'success_rate': success_rate
                }
                
                # Display the content immediately in human-readable format
                st.markdown("---")
                self.render_human_readable_content(all_content)
                
                # Show regeneration options
                self.render_regeneration_interface(content_display, selected_events)
                
            else:
                st.error("❌ No content was generated successfully")
                
        except Exception as e:
            st.error(f"❌ Content generation failed: {str(e)}")
            st.exception(e)
        
        finally:
            # Clear progress indicators
            main_progress.empty()
            main_status.empty()
    
    def render_human_readable_content(self, content_data):
        """Render generated content in human-readable format inline"""
        st.subheader("📄 Generated Social Media Content")
        
        if not content_data:
            st.warning("⚠️ No content to display")
            return
        
        # Group content by artist
        content_by_artist = {}
        for item in content_data:
            artist = item.get('artist_name', 'Unknown')
            if artist not in content_by_artist:
                content_by_artist[artist] = []
            content_by_artist[artist].append(item)
        
        # Summary
        total_pieces = len(content_data)
        unique_artists = len(content_by_artist)
        angles = list(set(item['content_angle'] for item in content_data))
        
        st.info(f"📊 **Summary:** {total_pieces} content pieces for {unique_artists} artists using {len(angles)} content angles")
        
        # Display content by artist
        for artist, items in content_by_artist.items():
            with st.expander(f"🎭 {artist.upper()} ({len(items)} pieces)", expanded=True):
                
                for i, item in enumerate(items, 1):
                    # Content angle header
                    angle_emoji = self.get_angle_emoji(item['content_angle'])
                    formatted_angle = item['content_angle'].replace('_', ' ').title()
                    
                    st.markdown(f"### {angle_emoji} {formatted_angle} - {item['event_name']}")
                    
                    # Platform and event info
                    col1, col2, col3 = st.columns([1, 1, 2])
                    with col1:
                        st.markdown(f"📱 **{item['platform'].title()}**")
                    with col2:
                        event_data = item.get('event_data', {})
                        rank = event_data.get('rank', 'N/A')
                        st.markdown(f"🏆 **Rank #{rank}**")
                    with col3:
                        location = f"{event_data.get('venue_city', 'Unknown')}, {event_data.get('venue_country', 'Unknown')}"
                        st.caption(f"📍 {location}")
                    
                    # Visual Text
                    if item.get('visual_text'):
                        # Check if this is an error message
                        if item.get('error') or item['visual_text'].startswith('❌'):
                            st.markdown("**❌ Content Generation Error:**")
                            st.error(item['visual_text'])
                            
                            # Show debug information if available
                            if item.get('debug_info'):
                                with st.expander("🐛 Debug Information", expanded=False):
                                    debug_info = item['debug_info']
                                    st.json(debug_info)
                                    
                                    st.markdown("**🔧 Troubleshooting:**")
                                    if debug_info.get('likely_cause'):
                                        st.markdown(f"- **Likely Cause**: {debug_info['likely_cause']}")
                                    
                                    if 'network' in debug_info.get('likely_cause', '').lower():
                                        st.markdown("- Contact your Snowflake admin about external API access")
                                        st.markdown("- Try the Connection Test page to verify API access")
                                    elif 'api key' in debug_info.get('likely_cause', '').lower():
                                        st.markdown("- Check Snowflake Secrets: OPENAI_API_KEY")
                                        st.markdown("- Verify your API key starts with 'sk-proj-'")
                                    elif 'model' in debug_info.get('likely_cause', '').lower():
                                        st.markdown("- Try a different model: gpt-4, gpt-3.5-turbo")
                                        st.markdown("- Check your OpenAI plan for model access")
                        else:
                            st.markdown("**🎯 Visual Text:**")
                            st.info(item['visual_text'])
                            
                            # Copy button for visual text
                            if st.button(f"📋 Copy Visual Text", key=f"copy_visual_{i}_{artist}_{item['content_angle']}", help="Copy visual text to clipboard"):
                                st.code(item['visual_text'])
                    
                    # Caption
                    if item.get('caption') and not item.get('error'):
                        st.markdown("**📝 Caption:**")
                        st.success(item['caption'])
                        
                        # Copy button for caption
                        if st.button(f"📋 Copy Caption", key=f"copy_caption_{i}_{artist}_{item['content_angle']}", help="Copy caption to clipboard"):
                            st.code(item['caption'])
                    
                    # Event metrics (collapsible)
                    with st.expander("📈 Event Metrics", expanded=False):
                        event_data = item.get('event_data', {})
                        
                        metric_col1, metric_col2, metric_col3 = st.columns(3)
                        
                        with metric_col1:
                            recent_gms = event_data.get('recent_7d_gms', 0)
                            if recent_gms:
                                st.metric("Recent 7d GMS", f"${recent_gms:,.0f}")
                            
                            career_multiple = event_data.get('career_context', {}).get('vs_career_avg_multiple', 0)
                            if career_multiple:
                                st.metric("vs Career Avg", f"{career_multiple:.1f}x")
                        
                        with metric_col2:
                            intl_pct = event_data.get('international_pct', 0)
                            if intl_pct:
                                st.metric("International %", f"{intl_pct:.0f}%")
                                
                            genre = event_data.get('genre', 'N/A')
                            st.metric("Genre", genre)
                        
                        with metric_col3:
                            quality_score = item.get('data_quality_score', 0)
                            if quality_score:
                                st.metric("Data Quality", f"{quality_score:.1%}")
                                
                            generated_at = item.get('generated_at', '')
                            if generated_at:
                                try:
                                    dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                                    st.metric("Generated", dt.strftime("%H:%M:%S"))
                                except:
                                    st.metric("Generated", "Now")
                    
                    st.markdown("---")
    
    def generate_single_content_piece(self, content_generator, event, angle, custom_prompts):
        """Generate a single piece of content with custom prompt support"""
        try:
            # Use custom prompts if available
            if custom_prompts and custom_prompts.get('user_prompt_template'):
                # TODO: Implement custom prompt integration with ContentGenerator
                # For now, use standard generation
                pass
            
            platform = custom_prompts.get('platform', 'instagram')
            content = content_generator.create_social_post(
                event_data=event,
                content_angle=angle,
                platform=platform
            )
            
            # Create content item
            content_item = {
                'event_id': event['event_id'],
                'artist_name': event.get('classified_artist_name', event.get('artist_name', 'Unknown')),
                'event_name': event['event_name'],
                'content_angle': angle,
                'platform': platform,
                'visual_text': content.get('visual_text', ''),
                'caption': content.get('caption', ''),
                'event_data': event,
                'generated_at': datetime.now().isoformat(),
                'data_quality_score': event['data_completeness']['completeness_score']
            }
            
            return content_item
            
        except Exception as e:
            raise Exception(f"Failed to generate {angle} content: {str(e)}")
    
    def update_content_display(self, placeholder, content_display):
        """Update the real-time content display"""
        display_html = ""
        
        for event_key, angles_data in content_display.items():
            display_html += f"<h4>🎵 {event_key}</h4>"
            
            for angle, data in angles_data.items():
                status = data['status']
                content = data['content']
                error = data['error']
                
                angle_name = angle.replace('_', ' ').title()
                display_html += f"<div style='margin-left: 20px; margin-bottom: 10px;'>"
                display_html += f"<strong>{angle_name}:</strong> {status}"
                
                if content:
                    visual_text = content.get('visual_text', '')[:100]
                    if len(visual_text) > 0:
                        display_html += f"<br><em>Preview: {visual_text}...</em>"
                
                if error:
                    display_html += f"<br><span style='color: red;'>Error: {error}</span>"
                
                display_html += "</div>"
            
            display_html += "<hr>"
        
        placeholder.markdown(display_html, unsafe_allow_html=True)
    
    def render_regeneration_interface(self, content_display, selected_events):
        """Render interface for regenerating individual content pieces"""
        st.markdown("### 🔄 Regenerate Content")
        
        with st.expander("♻️ Regenerate Individual Pieces", expanded=False):
            st.markdown("Select failed or unsatisfactory content pieces to regenerate:")
            
            failed_pieces = []
            for event_key, angles_data in content_display.items():
                for angle, data in angles_data.items():
                    if data['status'] in ['❌ Failed', '❌ Error']:
                        failed_pieces.append(f"{event_key} - {angle.replace('_', ' ').title()}")
            
            if failed_pieces:
                st.warning(f"⚠️ {len(failed_pieces)} pieces failed to generate")
                
                selected_failures = st.multiselect(
                    "Select pieces to regenerate:",
                    options=failed_pieces,
                    help="Choose which failed pieces to retry"
                )
                
                if selected_failures and st.button("🔄 Regenerate Selected", type="secondary"):
                    st.info("🔄 Regeneration feature coming soon!")
                    # TODO: Implement regeneration logic
            else:
                st.success("✅ All content pieces generated successfully!")
            
            # Manual regeneration option
            st.markdown("---")
            st.markdown("**Manual Regeneration:**")
            
            regenerate_event = st.selectbox(
                "Choose event:",
                options=range(len(selected_events)),
                format_func=lambda x: f"{selected_events[x].get('artist_name', 'Unknown')} - {selected_events[x]['event_name']}",
                help="Select event to regenerate content for"
            )
            
            if regenerate_event is not None:
                event = selected_events[regenerate_event]
                angles = self.pipeline.identify_content_angles(event)
                
                regenerate_angle = st.selectbox(
                    "Choose content angle:",
                    options=angles if angles else ['trending_event'],
                    format_func=lambda x: x.replace('_', ' ').title(),
                    help="Select content angle to regenerate"
                )
                
                if st.button("🎯 Regenerate This Piece", type="secondary"):
                    with st.spinner("Regenerating content..."):
                        try:
                            content_generator = ContentGenerator()
                            new_content = self.generate_single_content_piece(
                                content_generator, event, regenerate_angle, 
                                st.session_state.get('custom_prompts', {})
                            )
                            
                            if new_content:
                                st.success("✅ Content regenerated successfully!")
                                
                                # Display new content
                                st.markdown("**🎨 New Content:**")
                                st.markdown(f"**Visual Text:** {new_content.get('visual_text', 'N/A')}")
                                st.markdown(f"**Caption:** {new_content.get('caption', 'N/A')}")
                            else:
                                st.error("❌ Regeneration failed")
                                
                        except Exception as e:
                            st.error(f"❌ Regeneration error: {str(e)}")
    
    def test_snowflake_connection(self):
        """Test Snowflake connection"""
        if not self.snowflake_connector:
            return False, "Snowflake connector not initialized"
        
        try:
            if self.snowflake_connector.test_connection():
                return True, "Connection successful"
            else:
                return False, "Connection test failed"
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def render_sidebar(self):
        """Render sidebar navigation and settings with enhanced feedback"""
        st.sidebar.title("🎵 Social Content Generator")
        
        # Session status indicators
        self.render_session_status()
        
        st.sidebar.markdown("---")
        
        # Navigation
        st.sidebar.subheader("🧭 Navigation")
        page = st.sidebar.selectbox(
            "Choose Page",
            ["🏠 Dashboard", "🔧 Connection Test", "📊 Data Preview", "✍️ Generate Content", "📁 View Results"],
            help="Navigate between different sections of the app"
        )
        
        st.sidebar.markdown("---")
        
        # Settings
        st.sidebar.subheader("⚙️ Settings")
        
        max_events = st.sidebar.slider(
            "Max Events to Process",
            min_value=1,
            max_value=50,
            value=st.session_state.get('max_events', 10),
            help="Maximum number of events to process for content generation. Higher values take longer but provide more options."
        )
        st.session_state.max_events = max_events
        
        
        # Content generation settings
        with st.sidebar.expander("🤖 AI Settings", expanded=False):
            platform = st.selectbox(
                "Target Platform",
                ["TikTok", "Instagram", "Twitter"],
                index=0,
                help="Choose the social media platform to optimize content for"
            )
            st.session_state.target_platform = platform.lower()
            
            content_style = st.selectbox(
                "Content Style",
                ["Gen Z", "Professional", "Balanced"],
                index=0,
                help="Choose the tone and style for generated content"
            )
            st.session_state.content_style = content_style
        
        st.sidebar.markdown("---")
        
        # Quick actions
        st.sidebar.subheader("⚡ Quick Actions")
        
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            if st.button("🔄 Reset App", 
                        help="Clear all session data and start over",
                        use_container_width=True):
                self.reset_app_state()
        
        with col2:
            if st.button("🗑️ Clear Cache", 
                        help="Clear cached data to force refresh",
                        use_container_width=True):
                try:
                    st.cache_data.clear()
                    st.sidebar.success("✅ Cache cleared!")
                except Exception as e:
                    st.sidebar.error(f"❌ Error: {str(e)}")
        
        # Connection test
        if st.sidebar.button("🔌 Test Connections", 
                            help="Test Snowflake and OpenAI connections",
                            use_container_width=True):
            self.run_connection_tests()
        
        st.sidebar.markdown("---")
        
        # Help and info
        with st.sidebar.expander("📝 Help & Info", expanded=False):
            st.markdown("""
            **How to use this app:**
            1. Test connections first
            2. Load data from Snowflake  
            3. Select events for content
            4. Generate social media content
            5. Export and download results
            
            **Supported formats:**
            - JSON (structured data)
            - CSV (spreadsheet)
            - TXT (human readable)
            """)
        
        return page, max_events
    
    def render_session_status(self):
        """Show current session status in sidebar"""
        st.sidebar.subheader("📊 Session Status")
        
        # Data status
        if st.session_state.data_loaded:
            st.sidebar.success(f"✅ Data: {len(st.session_state.structured_events)} events")
        else:
            st.sidebar.warning("⚠️ No data loaded")
        
        # Selection status
        if st.session_state.selected_events:
            st.sidebar.info(f"🎯 Selected: {len(st.session_state.selected_events)} events")
        
        # Content status
        if st.session_state.content_generated:
            st.sidebar.success(f"✅ Content: {len(st.session_state.generated_content)} pieces")
        
        # Error status
        if st.session_state.last_error:
            st.sidebar.error(f"❌ Last error: {st.session_state.last_error[:50]}...")
    
    def run_connection_tests(self):
        """Test all connections and display results"""
        st.sidebar.markdown("**Testing connections...**")
        
        # Test Snowflake
        try:
            with st.spinner("Testing Snowflake..."):
                success, message = self.test_snowflake_connection()
                if success:
                    st.sidebar.success(f"✅ Snowflake: Connected")
                else:
                    st.sidebar.error(f"❌ Snowflake: {message}")
        except Exception as e:
            st.sidebar.error(f"❌ Snowflake: Error - {str(e)}")
        
        # Test OpenAI
        try:
            import os
            from openai import OpenAI
            
            # Get API key from environment or secrets
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key and hasattr(st, 'secrets') and 'OPENAI_API_KEY' in st.secrets:
                api_key = st.secrets['OPENAI_API_KEY']
            
            if api_key:
                with st.spinner("Testing OpenAI..."):
                    client = OpenAI(api_key=api_key)
                    # Get model from environment or secrets
                    model = os.getenv('OPENAI_MODEL')
                    if not model and hasattr(st, 'secrets') and 'OPENAI_MODEL' in st.secrets:
                        model = st.secrets['OPENAI_MODEL']
                    if not model:
                        model = 'gpt-4o'
                    
                    client.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": "test"}],
                        max_tokens=5
                    )
                    st.sidebar.success("✅ OpenAI: Connected")
            else:
                st.sidebar.error("❌ OpenAI: No API key found in environment or secrets")
        except Exception as e:
            st.sidebar.error(f"❌ OpenAI: {str(e)}")
    
    def render_dashboard(self):
        """Render main dashboard"""
        st.markdown("""
        <div class="main-header">
            <h1>🎵 Social Content Generator</h1>
            <p>Generate AI-powered social media content from top entertainment events</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Quick stats cards
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown("""
            <div class="metric-card">
                <h3>🎯 Data Source</h3>
                <p>Snowflake Views</p>
                <small>Top events data</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-card">
                <h3>🤖 AI Engine</h3>
                <p>OpenAI GPT</p>
                <small>Content generation</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-card">
                <h3>📱 Platforms</h3>
                <p>TikTok/Instagram</p>
                <small>Visual + Caption</small>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown("""
            <div class="metric-card">
                <h3>📊 Output</h3>
                <p>JSON + Text</p>
                <small>Structured data</small>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Quick start guide
        st.subheader("🚀 Quick Start Guide")
        
        # Key workflow
        st.markdown("""
        ### 📋 Basic Workflow
        1. **🔌 Test Connections**: Use sidebar → "Test Connections" to verify Snowflake & OpenAI
        2. **📊 Load Data**: Go to "Data Preview" → "Load Latest Data" to fetch events
        3. **🎯 Select Events**: Navigate to "Generate Content" and choose your events
        4. **✍️ Generate Content**: Run AI content generation with your selected events
        5. **📁 Export Results**: Download content in JSON, CSV, or TXT formats
        """)
        
        # Key enhancements
        st.markdown("### ✨ Key Features You Should Know")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **🔄 Smart Session Management**
            - Your data persists between page switches
            - Selected events remain chosen during generation
            - Settings are remembered across sessions
            
            **🛡️ Error Recovery**
            - Comprehensive error handling with clear messages
            - "Reset App" button to start fresh anytime
            - Automatic retry suggestions for failed operations
            
            **📊 Real-time Status**
            - Sidebar shows current session status
            - Live connection status indicators
            - Progress tracking during operations
            """)
        
        with col2:
            st.markdown("""
            **🤖 AI Customization**
            - Choose target platform (TikTok, Instagram, Twitter)
            - Select content style (Gen Z, Professional, Balanced)  
            - Custom prompt templates for different content angles
            
            **💡 Built-in Help**
            - Tooltips on all complex features
            - Expandable help sections throughout
            - Step-by-step guides and examples
            
            **⚡ Quick Actions**
            - One-click connection testing
            - Cache management controls  
            - Instant app reset functionality
            """)
        
        # Pro tips
        st.markdown("### 💪 Pro Tips")
        with st.expander("🎯 Advanced Usage Tips", expanded=False):
            st.markdown("""
            - **Use "Auto-refresh"** in Data Preview to automatically load fresh data
            - **Check Session Status** in sidebar to track your progress
            - **Export multiple formats** for different use cases (JSON for automation, TXT for review)
            - **Use Reset App** if you encounter any issues - it clears everything safely
            - **Test connections first** to avoid issues during content generation
            - **Select events strategically** - focus on high-performing or unique events
            - **Customize AI settings** based on your target audience and platform
            """)
        
        # Recent files
        if os.path.exists("data/generated_content"):
            files = [f for f in os.listdir("data/generated_content") if f.endswith('.json')]
            if files:
                st.subheader("📁 Recent Generated Content")
                latest_files = sorted(files, reverse=True)[:5]
                for file in latest_files:
                    st.text(f"📄 {file}")
    
    def render_connection_test(self):
        """Render connection test page"""
        st.header("🔧 Connection Test")
        
        st.markdown("Test your connections to ensure everything is working properly.")
        
        # Test both connections
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("🗄️ Test Snowflake", type="primary", use_container_width=True):
                with st.spinner("Testing Snowflake connection..."):
                    success, message = self.test_snowflake_connection()
                    
                    if success:
                        st.success(f"✅ Snowflake: {message}")
                        
                        # Test view access
                        with st.spinner("Testing view access..."):
                            try:
                                view_status = self.snowflake_connector.validate_views_exist()
                                
                                st.subheader("📊 View Access Status")
                                for view, accessible in view_status.items():
                                    if accessible:
                                        st.success(f"✅ {view}")
                                    else:
                                        st.error(f"❌ {view}")
                            except Exception as e:
                                st.error(f"❌ Failed to test view access: {str(e)}")
                    else:
                        st.error(f"❌ Snowflake: {message}")
        
        with col2:
            if st.button("🤖 Test OpenAI", type="secondary", use_container_width=True):
                self.test_openai_connection()
        
        # Test all connections at once
        st.markdown("---")
        if st.button("🔌 Test All Connections", use_container_width=True):
            st.markdown("### 🧪 Testing All Connections")
            
            # Test Snowflake
            st.markdown("**Testing Snowflake...**")
            success, message = self.test_snowflake_connection()
            if success:
                st.success(f"✅ Snowflake: {message}")
            else:
                st.error(f"❌ Snowflake: {message}")
            
            # Test OpenAI
            st.markdown("**Testing OpenAI...**")
            self.test_openai_connection()
    
    def test_openai_connection(self):
        """Test OpenAI API connection with comprehensive debugging"""
        st.markdown("#### 🔍 OpenAI Connection Diagnostics")
        
        # Debug info container
        debug_container = st.expander("🐛 Debug Information", expanded=True)
        
        try:
            import os
            from openai import OpenAI
            
            with debug_container:
                st.markdown("**Step 1: Environment Detection**")
                
                # Check environment type
                is_snowflake = 'SNOWFLAKE_WAREHOUSE' in os.environ or hasattr(st, 'secrets')
                environment = "Snowflake Streamlit" if is_snowflake else "Local Development"
                st.info(f"🌐 Environment: {environment}")
                
                # Check Python version and OpenAI library
                import sys
                import openai
                st.info(f"🐍 Python: {sys.version.split()[0]}")
                st.info(f"📦 OpenAI Library: {openai.__version__}")
                
                st.markdown("**Step 2: API Key Detection**")
                
                # Try environment variable first
                env_key = os.getenv('OPENAI_API_KEY')
                env_key_found = bool(env_key)
                st.info(f"🔑 Environment Variable: {'✅ Found' if env_key_found else '❌ Not Found'}")
                
                if env_key_found:
                    st.info(f"🔍 Key Length: {len(env_key)} characters")
                    st.info(f"🔍 Key Prefix: {env_key[:20]}...")
                
                # Try Streamlit secrets
                secrets_key = None
                secrets_key_found = False
                if hasattr(st, 'secrets'):
                    try:
                        secrets_key = st.secrets.get('OPENAI_API_KEY')
                        secrets_key_found = bool(secrets_key)
                        st.info(f"🔐 Streamlit Secrets: {'✅ Found' if secrets_key_found else '❌ Not Found'}")
                        
                        if secrets_key_found:
                            st.info(f"🔍 Secrets Key Length: {len(secrets_key)} characters")
                            st.info(f"🔍 Secrets Key Prefix: {secrets_key[:20]}...")
                            
                        # Show all available secrets (without values)
                        if hasattr(st.secrets, '_secrets'):
                            available_secrets = list(st.secrets._secrets.keys()) if st.secrets._secrets else []
                            st.info(f"🗝️ Available Secrets: {available_secrets}")
                        
                    except Exception as e:
                        st.warning(f"⚠️ Error accessing secrets: {str(e)}")
                else:
                    st.warning("⚠️ Streamlit secrets not available")
                
                # Try direct file reading as fallback
                st.markdown("**Step 2b: Direct File Access (Fallback)**")
                file_key = None
                for secrets_path in ['/.streamlit/secrets.toml', '/home/udf/.streamlit/secrets.toml']:
                    try:
                        if os.path.exists(secrets_path):
                            st.info(f"📁 Found secrets file: {secrets_path}")
                            with open(secrets_path, 'r') as f:
                                content = f.read()
                                st.info(f"📄 File content length: {len(content)} characters")
                                # Extract API key
                                import re
                                match = re.search(r'OPENAI_API_KEY\s*=\s*["\']([^"\']+)["\']', content)
                                if match:
                                    file_key = match.group(1)
                                    st.info(f"🔑 Found API key in file: {file_key[:20]}...")
                                    break
                                else:
                                    st.warning("⚠️ No API key pattern found in file")
                        else:
                            st.info(f"❌ File not found: {secrets_path}")
                    except Exception as e:
                        st.warning(f"⚠️ Error reading {secrets_path}: {str(e)}")
                
                # Determine which key to use
                api_key = env_key or secrets_key or file_key
                key_source = "Environment Variable" if env_key else "Streamlit Secrets" if secrets_key else "Direct File" if file_key else "None"
                st.info(f"🎯 Using Key From: {key_source}")
                
            if not api_key:
                st.error("❌ No API key found in environment variables or Streamlit secrets")
                with debug_container:
                    st.markdown("**🔧 Troubleshooting Steps:**")
                    st.markdown("1. In Snowflake, go to your Streamlit app settings")
                    st.markdown("2. Add a secret: `OPENAI_API_KEY = 'your-api-key-here'`")
                    st.markdown("3. Make sure the key starts with `sk-proj-`")
                    st.markdown("4. Redeploy your app")
                return
            
            with debug_container:
                st.markdown("**Step 3: Model Configuration**")
                
                # Get model from environment or secrets
                env_model = os.getenv('OPENAI_MODEL')
                secrets_model = None
                if hasattr(st, 'secrets'):
                    try:
                        secrets_model = st.secrets.get('OPENAI_MODEL')
                    except Exception as e:
                        st.info(f"⚠️ Could not access secrets for model: {str(e)}")
                
                model = env_model or secrets_model or 'gpt-4o'
                model_source = "Environment" if env_model else "Secrets" if secrets_model else "Default"
                
                st.info(f"🤖 Model: {model}")
                st.info(f"🎯 Model Source: {model_source}")
                
                st.markdown("**Step 4: API Connection Test**")
            
            with st.spinner("Testing OpenAI API connection..."):
                # Initialize client
                with debug_container:
                    st.info("🔌 Initializing OpenAI client...")
                
                client = OpenAI(api_key=api_key)
                
                with debug_container:
                    st.info("✅ Client initialized successfully")
                    st.info(f"📡 Making API call to model: {model}")
                
                # Test the connection
                response = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": "Say 'connection test successful'"}],
                    max_tokens=10,
                    timeout=30  # Add timeout
                )
                
                result = response.choices[0].message.content.strip()
                
                with debug_container:
                    st.info("✅ API call successful")
                    st.info(f"📝 Response ID: {response.id}")
                    st.info(f"📊 Usage: {response.usage}")
                
                st.success(f"✅ OpenAI: Connected successfully with {model}")
                st.info(f"💬 Test response: {result}")
                
        except ImportError as e:
            st.error(f"❌ Import Error: {str(e)}")
            with debug_container:
                st.markdown("**🔧 Fix:** Add `openai` to your requirements.txt file")
                
        except Exception as e:
            error_msg = str(e)
            st.error(f"❌ OpenAI Connection Failed: {error_msg}")
            
            with debug_container:
                st.markdown("**Step 5: Error Analysis**")
                st.code(error_msg)
                
                # Detailed error analysis
                if "rate_limit" in error_msg.lower():
                    st.markdown("**🔧 Rate Limit Error:**")
                    st.markdown("- You've exceeded your API quota")
                    st.markdown("- Wait a few minutes and try again")
                    st.markdown("- Check your OpenAI dashboard for usage")
                    
                elif "invalid_api_key" in error_msg.lower() or "unauthorized" in error_msg.lower():
                    st.markdown("**🔧 API Key Error:**")
                    st.markdown("- Your API key is invalid or expired")
                    st.markdown("- Generate a new key from OpenAI dashboard")
                    st.markdown("- Make sure it starts with `sk-proj-`")
                    
                elif "model" in error_msg.lower() and ("does not exist" in error_msg.lower() or "not found" in error_msg.lower()):
                    st.markdown(f"**🔧 Model Error:**")
                    st.markdown(f"- Model `{model}` is not available")
                    st.markdown("- Try `gpt-4`, `gpt-3.5-turbo`, or `gpt-4o-mini`")
                    st.markdown("- Check your OpenAI plan for model access")
                    
                elif "connection" in error_msg.lower() or "network" in error_msg.lower():
                    st.markdown("**🔧 Network Error:**")
                    st.markdown("- Snowflake might be blocking external API calls")
                    st.markdown("- Contact your Snowflake administrator")
                    st.markdown("- Check if OpenAI API is whitelisted")
                    
                elif "timeout" in error_msg.lower():
                    st.markdown("**🔧 Timeout Error:**")
                    st.markdown("- The API call took too long")
                    st.markdown("- This might indicate network restrictions")
                    st.markdown("- Try again or contact support")
                    
                elif "billing" in error_msg.lower() or "quota" in error_msg.lower():
                    st.markdown("**🔧 Billing Error:**")
                    st.markdown("- Your OpenAI account may need billing setup")
                    st.markdown("- Check your OpenAI dashboard for billing status")
                    st.markdown("- Add payment method if required")
                    
                else:
                    st.markdown("**🔧 Unknown Error:**")
                    st.markdown("- This is an unexpected error")
                    st.markdown("- Copy the error message above")
                    st.markdown("- Check OpenAI status page")
                    st.markdown("- Try with a different model")
                
                # Show full traceback for debugging
                import traceback
                st.markdown("**🔍 Full Traceback:**")
                st.code(traceback.format_exc())
    
    def render_data_preview(self):
        """Render data preview page with comprehensive loading section"""
        st.header("📊 Data Preview")
        
        # Use the new comprehensive data loading section
        self.render_data_loading_section()
    
    def render_generate_content(self, max_events):
        """Render content generation page"""
        st.header("✍️ Generate Content")
        
        # Check if data is available
        cached_data = self.get_cached_data()
        
        if not cached_data:
            st.warning("⚠️ No data loaded. Please load data first from the Data Preview page.")
            if st.button("🔄 Load Data Now", type="secondary"):
                with st.spinner("Loading data..."):
                    dataframes, error = self.load_snowflake_data()
                    if error:
                        st.error(f"❌ Failed to load data: {error}")
                    else:
                        st.session_state['loaded_data'] = dataframes
                        st.session_state['data_load_time'] = datetime.now()
                        st.rerun()
            return
        
        # Show data summary
        base_events = cached_data.get('base_events', pd.DataFrame())
        if not base_events.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Available Events", len(base_events))
            with col2:
                st.metric("Will Process", min(max_events, len(base_events)))
            with col3:
                load_time = st.session_state.get('data_load_time', 'Unknown')
                if isinstance(load_time, datetime):
                    st.metric("Data Age", f"{(datetime.now() - load_time).seconds // 60}m")
                else:
                    st.metric("Data Age", "Unknown")
        
        # Structure the events data first for selection
        try:
            events_data = self.pipeline.structure_event_data(cached_data)
            if not events_data:
                st.error("❌ No events available for selection")
                return
            
            # Create event selection interface
            selected_events = self.render_event_selection_interface(events_data, max_events)
            
            if not selected_events:
                st.info("🔍 Select events above to generate content")
                return
                
        except Exception as e:
            st.error(f"❌ Failed to structure event data: {str(e)}")
            return
        
        # Settings
        st.markdown("### ⚙️ Generation Settings")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Selected Events", len(selected_events))
        
        with col2:
            platform = st.session_state.get('target_platform', 'tiktok')
            st.metric("Target Platform", platform.title())
        
        with col3:
            total_angles = sum(len(self.pipeline.identify_content_angles(event)) for event in selected_events)
            st.metric("Content Pieces", total_angles)
        
        # Prompt Editing Section
        st.markdown("### ✏️ Prompt Editing")
        if selected_events:
            self.render_prompt_editing_section(selected_events)
        else:
            st.info("🔍 Select events above to customize prompts")
        
        st.markdown("---")
        
        # Generate content button
        if st.button("🚀 Generate Social Content", type="primary", use_container_width=True):
            self.run_enhanced_content_generation(selected_events)
    
    def render_view_results(self):
        """Render results viewing page with generated content from session state"""
        st.header("📁 View Results")
        
        # Check for generated content in session state
        if st.session_state.content_generated and st.session_state.generated_content:
            content_data = st.session_state.generated_content
            metadata = st.session_state.get('content_metadata', {})
            
            # Display metadata summary
            st.success(f"📊 Content Generated: {metadata.get('total_pieces', len(content_data))} pieces")
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Pieces", metadata.get('total_pieces', len(content_data)))
            with col2:
                st.metric("Events Covered", metadata.get('unique_events', 'N/A'))
            with col3:
                success_rate = metadata.get('success_rate', 0)
                st.metric("Success Rate", f"{success_rate:.1f}%" if success_rate else "N/A")
            
            # Display the content in human-readable format
            self.render_human_readable_content(content_data)
            
        else:
            st.info("🔍 No content has been generated yet. Use the 'Generate Content' page to create content.")
            
            # Show helpful guidance
            with st.expander("💡 How to Generate Content", expanded=True):
                st.markdown("""
                **To generate social media content:**
                
                1. **🔌 Test Connections** - Verify Snowflake and OpenAI are working
                2. **📊 Load Data** - Go to 'Data Preview' and load events from Snowflake
                3. **🎯 Select Events** - Navigate to 'Generate Content' and choose events
                4. **✍️ Generate** - Click the generate button to create content
                5. **👀 View Here** - Return to this page to see your generated content
                
                Generated content includes:
                - **Visual Text** for TikTok/Instagram assets
                - **Captions** for post descriptions
                - **Event metrics** and performance data
                - **Copy buttons** for easy use
                """)
    
    def render_enhanced_content_viewer(self, content_data, output_files):
        """Render enhanced content viewer with filtering and formatting"""
        
        if not content_data:
            st.warning("⚠️ No content data available")
            return
        
        st.success(f"📊 Latest Generated Content ({len(content_data)} pieces)")
        
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            unique_events = len(set(item.get('event_id', '') for item in content_data))
            st.metric("Unique Events", unique_events)
        
        with col2:
            unique_angles = len(set(item.get('content_angle', '') for item in content_data))
            st.metric("Content Angles", unique_angles)
        
        with col3:
            platforms = set(item.get('platform', 'unknown') for item in content_data)
            st.metric("Platforms", len(platforms))
        
        with col4:
            avg_quality = sum(item.get('data_quality_score', 0) for item in content_data) / len(content_data)
            st.metric("Avg Quality", f"{avg_quality:.1%}")
        
        st.markdown("---")
        
        # Filtering and sorting controls
        filtered_content = self.render_content_filters(content_data)
        
        st.markdown("---")
        
        # Content display options
        display_col1, display_col2 = st.columns([1, 1])
        
        with display_col1:
            display_mode = st.radio(
                "Display Mode:",
                ["Grid View", "List View", "Card View"],
                horizontal=True,
                help="Choose how to display content"
            )
        
        with display_col2:
            items_per_row = st.slider(
                "Items per row:",
                min_value=1,
                max_value=4,
                value=2,
                help="Number of content pieces per row (Grid View only)"
            ) if display_mode == "Grid View" else 2
        
        # Render content based on display mode
        if display_mode == "Grid View":
            self.render_grid_view(filtered_content, items_per_row)
        elif display_mode == "List View":
            self.render_list_view(filtered_content)
        else:  # Card View
            self.render_card_view(filtered_content)
        
        # Download section
        st.markdown("---")
        self.render_download_section(output_files)
    
    def render_content_filters(self, content_data):
        """Render filtering and sorting controls"""
        st.markdown("### 🔍 Filter & Sort")
        
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            # Artist filter
            all_artists = sorted(set(item.get('artist_name', 'Unknown') for item in content_data))
            selected_artists = st.multiselect(
                "Filter by Artist:",
                options=all_artists,
                default=all_artists,
                help="Select artists to display"
            )
        
        with filter_col2:
            # Content angle filter
            all_angles = sorted(set(item.get('content_angle', 'unknown') for item in content_data))
            selected_angles = st.multiselect(
                "Filter by Content Angle:",
                options=all_angles,
                default=all_angles,
                format_func=lambda x: x.replace('_', ' ').title(),
                help="Select content angles to display"
            )
        
        with filter_col3:
            # Sort options
            sort_options = [
                ("artist_name", "Artist Name"),
                ("content_angle", "Content Angle"),
                ("data_quality_score", "Quality Score"),
                ("generated_at", "Generation Time")
            ]
            
            sort_by = st.selectbox(
                "Sort by:",
                options=[x[0] for x in sort_options],
                format_func=lambda x: next(y[1] for y in sort_options if y[0] == x),
                help="Choose sorting criteria"
            )
            
            sort_desc = st.checkbox("Descending", value=True, help="Sort in descending order")
        
        # Apply filters
        filtered_content = [
            item for item in content_data
            if item.get('artist_name', 'Unknown') in selected_artists
            and item.get('content_angle', 'unknown') in selected_angles
        ]
        
        # Apply sorting
        if sort_by:
            filtered_content = sorted(
                filtered_content,
                key=lambda x: x.get(sort_by, ''),
                reverse=sort_desc
            )
        
        st.info(f"Showing {len(filtered_content)} of {len(content_data)} content pieces")
        
        return filtered_content
    
    def render_grid_view(self, content_data, items_per_row):
        """Render content in grid layout"""
        st.markdown("### 🎨 Content Grid")
        
        for i in range(0, len(content_data), items_per_row):
            cols = st.columns(items_per_row)
            
            for j, col in enumerate(cols):
                if i + j < len(content_data):
                    item = content_data[i + j]
                    
                    with col:
                        self.render_content_card(item, i + j + 1)
    
    def render_list_view(self, content_data):
        """Render content in list layout"""
        st.markdown("### 📋 Content List")
        
        for i, item in enumerate(content_data, 1):
            with st.expander(f"#{i} {item.get('artist_name', 'Unknown')} - {item.get('content_angle', 'unknown').replace('_', ' ').title()}", expanded=False):
                self.render_content_details(item, i)
    
    def render_card_view(self, content_data):
        """Render content in card layout"""
        st.markdown("### 🃏 Content Cards")
        
        for i, item in enumerate(content_data, 1):
            self.render_content_card(item, i, expanded=True)
            st.markdown("---")
    
    def render_content_card(self, item, index, expanded=False):
        """Render individual content card"""
        
        # Card header
        artist_name = item.get('artist_name', 'Unknown')
        content_angle = item.get('content_angle', 'unknown').replace('_', ' ').title()
        platform = item.get('platform', 'unknown').title()
        
        st.markdown(f"**#{index} {artist_name}** • {content_angle} • {platform}")
        
        # Visual text container
        visual_text = item.get('visual_text', 'No visual text')
        st.markdown("**🎨 Visual Text:**")
        
        visual_container = st.container()
        with visual_container:
            st.markdown(
                f"""<div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 10px;
                    font-weight: bold;
                    text-align: center;
                    font-size: 18px;
                    margin: 10px 0;
                ">{visual_text}</div>""",
                unsafe_allow_html=True
            )
        
        # Copy button for visual text
        if st.button(f"📋 Copy Visual Text", key=f"copy_visual_{index}", help="Copy visual text to clipboard"):
            st.code(visual_text, language="text")
            st.success("✅ Visual text ready to copy!")
        
        # Caption container
        caption = item.get('caption', 'No caption')
        st.markdown("**📝 Caption:**")
        
        st.markdown(
            f"""<div style="
                background: #f8f9ff;
                border: 1px solid #e0e7ff;
                padding: 15px;
                border-radius: 8px;
                margin: 10px 0;
                font-style: italic;
            ">{caption}</div>""",
            unsafe_allow_html=True
        )
        
        # Copy button for caption
        if st.button(f"📋 Copy Caption", key=f"copy_caption_{index}", help="Copy caption to clipboard"):
            st.code(caption, language="text")
            st.success("✅ Caption ready to copy!")
        
        # Metadata section
        if expanded:
            self.render_content_metadata(item)
    
    def render_content_details(self, item, index):
        """Render detailed content view for list mode"""
        
        # Visual text and caption in columns
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**🎨 Visual Text:**")
            visual_text = item.get('visual_text', 'No visual text')
            st.markdown(
                f"""<div style="
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 15px;
                    border-radius: 8px;
                    font-weight: bold;
                    text-align: center;
                ">{visual_text}</div>""",
                unsafe_allow_html=True
            )
            
            if st.button(f"📋 Copy Visual", key=f"copy_visual_list_{index}"):
                st.code(visual_text, language="text")
        
        with col2:
            st.markdown("**📝 Caption:**")
            caption = item.get('caption', 'No caption')
            st.markdown(
                f"""<div style="
                    background: #f8f9ff;
                    border: 1px solid #e0e7ff;
                    padding: 15px;
                    border-radius: 8px;
                    font-style: italic;
                ">{caption}</div>""",
                unsafe_allow_html=True
            )
            
            if st.button(f"📋 Copy Caption", key=f"copy_caption_list_{index}"):
                st.code(caption, language="text")
        
        # Full content copy
        st.markdown("**📄 Full Content:**")
        full_content = f"Visual Text:\n{visual_text}\n\nCaption:\n{caption}"
        
        if st.button(f"📋 Copy Both", key=f"copy_both_{index}", help="Copy both visual text and caption"):
            st.code(full_content, language="text")
            st.success("✅ Full content ready to copy!")
        
        # Metadata
        self.render_content_metadata(item)
    
    def render_content_metadata(self, item):
        """Render content metadata"""
        st.markdown("**📊 Metadata:**")
        
        metadata_col1, metadata_col2 = st.columns(2)
        
        with metadata_col1:
            st.text(f"Event: {item.get('event_name', 'Unknown')}")
            st.text(f"Platform: {item.get('platform', 'unknown').title()}")
            st.text(f"Quality Score: {item.get('data_quality_score', 0):.1%}")
        
        with metadata_col2:
            generated_at = item.get('generated_at', 'Unknown')
            if generated_at != 'Unknown':
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                    st.text(f"Generated: {dt.strftime('%Y-%m-%d %H:%M')}")
                except:
                    st.text(f"Generated: {generated_at}")
            else:
                st.text("Generated: Unknown")
            
            # Priority score (calculated)
            priority_score = self.calculate_priority_score(item)
            st.text(f"Priority Score: {priority_score}/10")
    
    def calculate_priority_score(self, item):
        """Calculate priority score based on content angle and quality"""
        base_score = item.get('data_quality_score', 0) * 5  # 0-5 points for quality
        
        angle_scores = {
            'major_spike': 5,
            'significant_spike': 4,
            'genre_leader': 4,
            'international_phenomenon': 4,
            'tour_standout': 3,
            'top_performer': 3,
            'international_appeal': 2,
            'pricing_surge': 2,
            'notable_performance': 2,
            'demand_indicator': 1,
            'top_performance': 1,
            'trending_event': 1
        }
        
        angle_score = angle_scores.get(item.get('content_angle', 'trending_event'), 1)
        
        total_score = min(10, int(base_score + angle_score))
        return total_score
    
    def render_download_section(self, output_files):
        """Render enhanced download section with multiple export options"""
        st.markdown("### 📥 Export & Download Options")
        
        # Get content data for exports
        content_data = st.session_state.get('latest_content', [])
        
        if not content_data:
            st.warning("⚠️ No content data available for export")
            return
        
        # Generate timestamped filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Export options in columns
        export_col1, export_col2, export_col3 = st.columns(3)
        
        with export_col1:
            st.markdown("**📄 JSON Export**")
            json_data = self.prepare_json_export(content_data)
            json_filename = f"social_content_export_{timestamp}.json"
            
            st.download_button(
                label="📥 Download JSON",
                data=json_data,
                file_name=json_filename,
                mime="application/json",
                help="Download structured data for APIs and automation"
            )
            
            # Save to directory option
            if st.button("💾 Save JSON to Directory", key="save_json"):
                self.save_to_directory(json_data, json_filename, 'json')
        
        with export_col2:
            st.markdown("**📊 CSV Export**")
            csv_data = self.prepare_csv_export(content_data)
            csv_filename = f"social_content_export_{timestamp}.csv"
            
            st.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name=csv_filename,
                mime="text/csv",
                help="Download spreadsheet format for analysis"
            )
            
            # Save to directory option
            if st.button("💾 Save CSV to Directory", key="save_csv"):
                self.save_to_directory(csv_data, csv_filename, 'csv')
        
        with export_col3:
            st.markdown("**📝 Text Export**")
            text_data = self.prepare_text_export(content_data)
            text_filename = f"social_content_export_{timestamp}.txt"
            
            st.download_button(
                label="📥 Download Text",
                data=text_data,
                file_name=text_filename,
                mime="text/plain",
                help="Download human-readable format"
            )
            
            # Save to directory option
            if st.button("💾 Save Text to Directory", key="save_text"):
                self.save_to_directory(text_data, text_filename, 'txt')
        
        st.markdown("---")
        
        # Original files download (if available)
        if output_files and os.path.exists(output_files.get('json_file', '')):
            st.markdown("### 📁 Original Generated Files")
            
            original_col1, original_col2 = st.columns(2)
            
            with original_col1:
                st.markdown("**🔄 Original JSON**")
                with open(output_files['json_file'], 'r') as f:
                    original_json = f.read()
                
                st.download_button(
                    label="📥 Download Original JSON",
                    data=original_json,
                    file_name=os.path.basename(output_files['json_file']),
                    mime="application/json",
                    help="Download the original generated JSON file"
                )
                
                st.text(f"📍 {output_files['json_file']}")
            
            with original_col2:
                if os.path.exists(output_files.get('text_file', '')):
                    st.markdown("**🔄 Original Text**")
                    with open(output_files['text_file'], 'r') as f:
                        original_text = f.read()
                    
                    st.download_button(
                        label="📥 Download Original Text",
                        data=original_text,
                        file_name=os.path.basename(output_files['text_file']),
                        mime="text/plain",
                        help="Download the original generated text file"
                    )
                    
                    st.text(f"📍 {output_files['text_file']}")
        
        # Bulk export options
        st.markdown("---")
        st.markdown("### 📦 Bulk Export Options")
        
        bulk_col1, bulk_col2 = st.columns(2)
        
        with bulk_col1:
            if st.button("📦 Export All Formats", type="secondary"):
                self.export_all_formats(content_data, timestamp)
        
        with bulk_col2:
            # Custom format selection
            selected_formats = st.multiselect(
                "Select formats to export:",
                options=['JSON', 'CSV', 'TXT'],
                default=['JSON', 'CSV'],
                help="Choose which formats to export together"
            )
            
            if selected_formats and st.button("🎯 Export Selected", type="secondary"):
                self.export_selected_formats(content_data, timestamp, selected_formats)
        
        # Show recent exports
        self.show_recent_exports()
    
    def prepare_json_export(self, content_data):
        """Prepare JSON export with metadata"""
        export_data = {
            'metadata': {
                'exported_at': datetime.now().isoformat(),
                'total_content_pieces': len(content_data),
                'unique_events': len(set(item.get('event_id', '') for item in content_data)),
                'unique_artists': len(set(item.get('artist_name', '') for item in content_data)),
                'content_angles': list(set(item.get('content_angle', '') for item in content_data)),
                'platforms': list(set(item.get('platform', '') for item in content_data)),
                'export_version': '1.0',
                'source': 'Social Content Generator'
            },
            'content': content_data,
            'summary': {
                'average_quality_score': sum(item.get('data_quality_score', 0) for item in content_data) / len(content_data) if content_data else 0,
                'priority_distribution': self.calculate_priority_distribution(content_data),
                'angle_distribution': self.calculate_angle_distribution(content_data)
            }
        }
        
        return json.dumps(export_data, indent=2, ensure_ascii=False, default=str)
    
    def prepare_csv_export(self, content_data):
        """Prepare CSV export with flattened data"""
        import csv
        import io
        
        output = io.StringIO()
        
        # Define CSV headers
        headers = [
            'Index',
            'Artist_Name',
            'Event_Name', 
            'Content_Angle',
            'Platform',
            'Visual_Text',
            'Caption',
            'Priority_Score',
            'Quality_Score',
            'Generated_At',
            'Event_ID',
            'Event_City',
            'Event_Country',
            'Event_Genre',
            'Event_Rank'
        ]
        
        writer = csv.writer(output)
        writer.writerow(headers)
        
        for i, item in enumerate(content_data, 1):
            event_data = item.get('event_data', {})
            
            row = [
                i,
                item.get('artist_name', 'Unknown'),
                item.get('event_name', 'Unknown'),
                item.get('content_angle', 'unknown').replace('_', ' ').title(),
                item.get('platform', 'unknown').title(),
                item.get('visual_text', '').replace('\n', ' | '),  # Replace newlines for CSV
                item.get('caption', '').replace('\n', ' | '),
                self.calculate_priority_score(item),
                f"{item.get('data_quality_score', 0):.1%}",
                item.get('generated_at', 'Unknown'),
                item.get('event_id', 'Unknown'),
                event_data.get('venue_city', 'Unknown'),
                event_data.get('venue_country', 'Unknown'),
                event_data.get('genre', 'Unknown'),
                event_data.get('rank', 'Unknown')
            ]
            
            writer.writerow(row)
        
        return output.getvalue()
    
    def prepare_text_export(self, content_data):
        """Prepare human-readable text export"""
        output = []
        
        # Header
        output.append("🎵 SOCIAL MEDIA CONTENT EXPORT")
        output.append("=" * 60)
        output.append(f"Exported: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append(f"Total Pieces: {len(content_data)}")
        output.append(f"Unique Events: {len(set(item.get('event_id', '') for item in content_data))}")
        output.append("")
        
        # Group by artist
        content_by_artist = {}
        for item in content_data:
            artist = item.get('artist_name', 'Unknown')
            if artist not in content_by_artist:
                content_by_artist[artist] = []
            content_by_artist[artist].append(item)
        
        # Content sections
        for artist, items in sorted(content_by_artist.items()):
            output.append(f"🎭 {artist.upper()}")
            output.append("-" * 40)
            output.append("")
            
            for i, item in enumerate(items, 1):
                angle = item.get('content_angle', 'unknown').replace('_', ' ').title()
                platform = item.get('platform', 'unknown').title()
                priority = self.calculate_priority_score(item)
                
                output.append(f"[{i}] {angle} • {platform} • Priority: {priority}/10")
                output.append("")
                
                output.append("🎨 VISUAL TEXT:")
                output.append(item.get('visual_text', 'No visual text'))
                output.append("")
                
                output.append("📝 CAPTION:")
                output.append(item.get('caption', 'No caption'))
                output.append("")
                
                output.append("📊 METADATA:")
                output.append(f"Event: {item.get('event_name', 'Unknown')}")
                output.append(f"Quality: {item.get('data_quality_score', 0):.1%}")
                output.append(f"Generated: {item.get('generated_at', 'Unknown')}")
                output.append("")
                output.append("~" * 50)
                output.append("")
            
            output.append("")
        
        return "\n".join(output)
    
    def save_to_directory(self, data, filename, file_type):
        """Save export data to the generated_content directory"""
        try:
            # Ensure directory exists
            os.makedirs("data/generated_content", exist_ok=True)
            
            # Full file path
            file_path = os.path.join("data/generated_content", filename)
            
            # Write file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(data)
            
            st.success(f"✅ {file_type.upper()} saved to: `{file_path}`")
            st.info(f"📁 File size: {len(data.encode('utf-8')) / 1024:.1f} KB")
            
        except Exception as e:
            st.error(f"❌ Failed to save {file_type.upper()}: {str(e)}")
    
    def export_all_formats(self, content_data, timestamp):
        """Export all formats at once"""
        try:
            # Prepare all exports
            json_data = self.prepare_json_export(content_data)
            csv_data = self.prepare_csv_export(content_data)
            text_data = self.prepare_text_export(content_data)
            
            # Save all files
            base_name = f"social_content_export_{timestamp}"
            
            self.save_to_directory(json_data, f"{base_name}.json", 'json')
            self.save_to_directory(csv_data, f"{base_name}.csv", 'csv')
            self.save_to_directory(text_data, f"{base_name}.txt", 'txt')
            
            st.success("🎉 All formats exported successfully!")
            
        except Exception as e:
            st.error(f"❌ Bulk export failed: {str(e)}")
    
    def export_selected_formats(self, content_data, timestamp, selected_formats):
        """Export only selected formats"""
        try:
            base_name = f"social_content_export_{timestamp}"
            exported = []
            
            for format_type in selected_formats:
                if format_type == 'JSON':
                    data = self.prepare_json_export(content_data)
                    self.save_to_directory(data, f"{base_name}.json", 'json')
                    exported.append('JSON')
                
                elif format_type == 'CSV':
                    data = self.prepare_csv_export(content_data)
                    self.save_to_directory(data, f"{base_name}.csv", 'csv')
                    exported.append('CSV')
                
                elif format_type == 'TXT':
                    data = self.prepare_text_export(content_data)
                    self.save_to_directory(data, f"{base_name}.txt", 'txt')
                    exported.append('TXT')
            
            st.success(f"✅ Exported formats: {', '.join(exported)}")
            
        except Exception as e:
            st.error(f"❌ Selected export failed: {str(e)}")
    
    def calculate_priority_distribution(self, content_data):
        """Calculate priority score distribution"""
        distribution = {}
        for item in content_data:
            score = self.calculate_priority_score(item)
            distribution[score] = distribution.get(score, 0) + 1
        return distribution
    
    def calculate_angle_distribution(self, content_data):
        """Calculate content angle distribution"""
        distribution = {}
        for item in content_data:
            angle = item.get('content_angle', 'unknown')
            distribution[angle] = distribution.get(angle, 0) + 1
        return distribution
    
    def show_recent_exports(self):
        """Show recently exported files"""
        st.markdown("### 📋 Recent Exports")
        
        if os.path.exists("data/generated_content"):
            files = [f for f in os.listdir("data/generated_content") 
                    if f.startswith('social_content_export_') and f.endswith(('.json', '.csv', '.txt'))]
            
            if files:
                # Group by timestamp
                export_groups = {}
                for file in files:
                    # Extract timestamp
                    parts = file.split('_')
                    if len(parts) >= 4:
                        timestamp = f"{parts[2]}_{parts[3].split('.')[0]}"
                        if timestamp not in export_groups:
                            export_groups[timestamp] = []
                        export_groups[timestamp].append(file)
                
                # Show recent exports (last 5)
                for timestamp in sorted(export_groups.keys(), reverse=True)[:5]:
                    with st.expander(f"📅 Exported: {timestamp}", expanded=False):
                        for file in sorted(export_groups[timestamp]):
                            file_path = os.path.join("data/generated_content", file)
                            file_size = os.path.getsize(file_path) / 1024
                            
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.text(f"📄 {file}")
                            
                            with col2:
                                st.text(f"{file_size:.1f} KB")
                            
                            with col3:
                                # Download button for each export
                                with open(file_path, 'r', encoding='utf-8') as f:
                                    file_content = f.read()
                                
                                if file.endswith('.json'):
                                    mime_type = "application/json"
                                elif file.endswith('.csv'):
                                    mime_type = "text/csv"
                                else:
                                    mime_type = "text/plain"
                                
                                st.download_button(
                                    label="📥",
                                    data=file_content,
                                    file_name=file,
                                    mime=mime_type,
                                    key=f"download_export_{file}"
                                )
            else:
                st.info("No recent exports found")
        else:
            st.info("Export directory not found")
    
    def render_available_files_section(self):
        """Render available files section"""
        st.markdown("---")
        st.markdown("### 📚 Available Files")
        
        if os.path.exists("data/generated_content"):
            files = [f for f in os.listdir("data/generated_content") if f.endswith(('.json', '.txt'))]
            if files:
                
                # Group files by timestamp
                file_groups = {}
                for file in files:
                    # Extract timestamp from filename
                    if 'social_content_' in file:
                        timestamp = file.replace('social_content_', '').split('.')[0]
                        if timestamp not in file_groups:
                            file_groups[timestamp] = []
                        file_groups[timestamp].append(file)
                
                for timestamp, group_files in sorted(file_groups.items(), reverse=True):
                    with st.expander(f"📅 Generated on {timestamp}", expanded=False):
                        for file in sorted(group_files):
                            file_path = os.path.join("data/generated_content", file)
                            file_size = os.path.getsize(file_path) / 1024  # KB
                            
                            col1, col2, col3 = st.columns([2, 1, 1])
                            
                            with col1:
                                st.text(f"📄 {file}")
                            
                            with col2:
                                st.text(f"{file_size:.1f} KB")
                            
                            with col3:
                                if file.endswith('.json'):
                                    if st.button(f"👁️ View", key=f"view_{file}"):
                                        # Load and display this file's content
                                        with open(file_path, 'r') as f:
                                            json_data = json.load(f)
                                        
                                        st.session_state['latest_content'] = json_data.get('content', [])
                                        st.session_state['latest_output'] = {
                                            'json_file': file_path,
                                            'text_file': file_path.replace('.json', '.txt')
                                        }
                                        st.rerun()
            else:
                st.info("No generated content files found")
        else:
            st.info("Content directory not found")
    
    def run(self):
        """Run the Streamlit application with comprehensive error handling"""
        try:
            # Load custom CSS
            load_custom_css()
            
            # Check for initialization errors
            if st.session_state.last_error:
                st.error(f"❌ Initialization Error: {st.session_state.last_error}")
                st.info("🔄 Try refreshing the page or check your environment configuration.")
                
                # Show reset button for recovery
                if st.button("🔄 Reset Application", type="primary"):
                    self.reset_app_state()
                return
            
            # Render sidebar and get settings
            try:
                page, max_events = self.render_sidebar()
            except Exception as e:
                st.error(f"❌ Sidebar rendering error: {str(e)}")
                return
            
            # Route to appropriate page with error handling
            try:
                if page == "🏠 Dashboard":
                    self.render_dashboard()
                elif page == "🔧 Connection Test":
                    self.render_connection_test()
                elif page == "📊 Data Preview":
                    self.render_data_preview()
                elif page == "✍️ Generate Content":
                    self.render_generate_content(max_events)
                elif page == "📁 View Results":
                    self.render_view_results()
                else:
                    st.error(f"❌ Unknown page: {page}")
                    
            except Exception as e:
                st.error(f"❌ Page rendering error: {str(e)}")
                st.info("🔄 Try switching to a different page or refresh the application.")
                
                # Show detailed error in expander for debugging
                with st.expander("🔍 Error Details", expanded=False):
                    st.code(str(e))
                    import traceback
                    st.code(traceback.format_exc())
        
        except Exception as e:
            st.error(f"❌ Critical application error: {str(e)}")
            st.info("🔄 Please refresh the page and try again.")

# Initialize and run the app
def main():
    """Main application entry point with comprehensive error handling"""
    try:
        # Initialize app
        with st.spinner("🚀 Initializing Social Content Generator..."):
            app = SocialContentApp()
        
        # Run the application
        app.run()
        
    except ImportError as e:
        st.error("❌ Missing Dependencies")
        st.markdown("""
        **Required packages are missing. Please install:**
        ```bash
        pip install streamlit pandas snowflake-connector-python openai python-dotenv
        ```
        
        **Error details:** `{}`
        """.format(str(e)))
        
    except FileNotFoundError as e:
        st.error("❌ File Not Found")
        st.markdown("""
        **Missing required files. Please ensure:**
        - Your project structure is complete
        - All source files are in the correct directories
        - Your `.env` file contains required credentials
        
        **Error details:** `{}`
        """.format(str(e)))
        
    except Exception as e:
        st.error("❌ Application Initialization Failed")
        st.markdown("""
        **Something went wrong during startup.**
        
        **Troubleshooting steps:**
        1. Check your internet connection
        2. Verify your Snowflake credentials in `.env`
        3. Ensure OpenAI API key is valid
        4. Try refreshing the page
        
        **Error details:** `{}`
        """.format(str(e)))
        
        # Show detailed error for debugging
        with st.expander("🔍 Full Error Traceback", expanded=False):
            import traceback
            st.code(traceback.format_exc())
        
        # Recovery options
        st.markdown("### 🔄 Recovery Options")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Refresh Page", type="primary"):
                st.experimental_rerun()
        
        with col2:
            if st.button("🛠️ Clear Cache"):
                st.cache_data.clear()
                st.success("✅ Cache cleared! Please refresh.")
        
        with col3:
            st.markdown("[📖 Documentation](https://docs.streamlit.io)")

if __name__ == "__main__":
    main()