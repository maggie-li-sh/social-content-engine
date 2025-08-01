"""
AI Contextualizer - Enhanced with Social Media Content Generation
"""

import os
from typing import Dict, List, Optional
import openai
from dotenv import load_dotenv

# Try to load .env file (for local development)
try:
    load_dotenv()
except:
    pass

class ContentGenerator:
    def __init__(self):
        """Initialize the content generator with OpenAI client"""
        # Try to get API key from multiple sources
        api_key = self._get_api_key()
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found. Please set it in Snowflake secrets or environment variables.")
        
        self.client = openai.OpenAI(api_key=api_key)
        self.model = self._get_model()
        
        # Content templates for different angles
        self.angle_templates = {
            'major_spike': self._create_major_spike_template,
            'significant_spike': self._create_significant_spike_template,
            'notable_performance': self._create_notable_performance_template,
            'international_phenomenon': self._create_international_phenomenon_template,
            'international_appeal': self._create_international_appeal_template,
            'genre_leader': self._create_genre_leader_template,
            'top_performer': self._create_top_performer_template,
            'pricing_surge': self._create_pricing_surge_template,
            'demand_indicator': self._create_demand_indicator_template,
            'tour_standout': self._create_tour_standout_template,
            'top_performance': self._create_top_performance_template,
            'trending_event': self._create_trending_event_template
        }
    
    def _get_api_key(self):
        """Get OpenAI API key from environment or Snowflake secrets"""
        # Try environment variable first (local development)
        api_key = os.getenv('OPENAI_API_KEY')
        if api_key:
            return api_key
        
        # Try Snowflake secrets
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'OPENAI_API_KEY' in st.secrets:
                return st.secrets['OPENAI_API_KEY']
        except:
            pass
        
        # Try reading secrets.toml directly from known Snowflake paths
        for secrets_path in ['/.streamlit/secrets.toml', '/home/udf/.streamlit/secrets.toml', 'secrets.toml']:
            try:
                if os.path.exists(secrets_path):
                    import toml
                    secrets = toml.load(secrets_path)
                    api_key = secrets.get('OPENAI_API_KEY')
                    if api_key:
                        return api_key
            except Exception:
                continue
        
        # Fallback: try reading as plain text file
        for secrets_path in ['/.streamlit/secrets.toml', '/home/udf/.streamlit/secrets.toml']:
            try:
                if os.path.exists(secrets_path):
                    with open(secrets_path, 'r') as f:
                        content = f.read()
                        # Simple regex to extract API key
                        import re
                        match = re.search(r'OPENAI_API_KEY\s*=\s*["\']([^"\']+)["\']', content)
                        if match:
                            return match.group(1)
            except Exception:
                continue
        
        return None
    
    def _get_model(self):
        """Get OpenAI model from environment or Snowflake secrets"""
        # Try environment variable first
        model = os.getenv('OPENAI_MODEL')
        if model:
            return model
        
        # Try Snowflake secrets
        try:
            import streamlit as st
            if hasattr(st, 'secrets') and 'OPENAI_MODEL' in st.secrets:
                return st.secrets['OPENAI_MODEL']
        except:
            pass
        
        # Try reading secrets.toml directly from known Snowflake paths
        for secrets_path in ['/.streamlit/secrets.toml', '/home/udf/.streamlit/secrets.toml', 'secrets.toml']:
            try:
                if os.path.exists(secrets_path):
                    import toml
                    secrets = toml.load(secrets_path)
                    model = secrets.get('OPENAI_MODEL')
                    if model:
                        return model
            except Exception:
                continue
        
        # Fallback: try reading as plain text file
        for secrets_path in ['/.streamlit/secrets.toml', '/home/udf/.streamlit/secrets.toml']:
            try:
                if os.path.exists(secrets_path):
                    with open(secrets_path, 'r') as f:
                        content = f.read()
                        # Simple regex to extract model
                        import re
                        match = re.search(r'OPENAI_MODEL\s*=\s*["\']([^"\']+)["\']', content)
                        if match:
                            return match.group(1)
            except Exception:
                continue
        
        # Default to gpt-4o
        return 'gpt-4o'
    
    def create_social_post(self, event_data: Dict, content_angle: str, platform: str = 'instagram') -> Dict:
        """Generate social media content for a specific event and angle"""
        
        # Get the appropriate template
        template_func = self.angle_templates.get(content_angle, self._create_default_template)
        prompt = template_func(event_data, platform)
        
        # Generate content using ChatGPT
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system", 
                        "content": self._get_system_prompt(platform)
                    },
                    {
                        "role": "user", 
                        "content": prompt
                    }
                ],
                max_tokens=600,
                temperature=0.7
            )
            
            content = response.choices[0].message.content.strip()
            
            # Parse the response into visual text and caption
            return self._parse_dual_content(content, platform)
            
        except Exception as e:
            error_msg = str(e)
            
            # Enhanced debugging information
            debug_info = {
                'original_error': error_msg,
                'model_used': self.model,
                'platform': platform,
                'content_angle': content_angle,
                'event_artist': event_data.get('classified_artist_name', 'Unknown')
            }
            
            # Provide more specific error messages
            if "rate_limit" in error_msg.lower():
                user_msg = "Rate limit exceeded. Please wait a moment and try again."
                debug_info['likely_cause'] = "API rate limit hit - too many requests"
            elif "invalid_api_key" in error_msg.lower() or "unauthorized" in error_msg.lower():
                user_msg = "Invalid API key. Please check your OpenAI API key in Snowflake secrets."
                debug_info['likely_cause'] = "API key not set properly in Snowflake"
            elif "model" in error_msg.lower() and "does not exist" in error_msg.lower():
                user_msg = f"Model '{self.model}' not available. Check your OpenAI plan or use a different model."
                debug_info['likely_cause'] = f"Model {self.model} not accessible with your API key"
            elif "connection" in error_msg.lower() or "network" in error_msg.lower():
                user_msg = "Network connection error. Snowflake may be blocking external API calls."
                debug_info['likely_cause'] = "Network restrictions in Snowflake environment"
            elif "timeout" in error_msg.lower():
                user_msg = "Request timeout. This may indicate network restrictions."
                debug_info['likely_cause'] = "API call timed out - possible network restrictions"
            elif "billing" in error_msg.lower() or "quota" in error_msg.lower():
                user_msg = "Billing or quota issue. Check your OpenAI account status."
                debug_info['likely_cause'] = "OpenAI account billing or usage limits"
            else:
                user_msg = f"OpenAI API error: {error_msg}"
                debug_info['likely_cause'] = "Unknown error - check OpenAI status"
            
            return {
                'visual_text': f"❌ {user_msg}",
                'caption': f"❌ {user_msg}",
                'platform': platform,
                'error': True,
                'debug_info': debug_info
            }
    
    def _parse_dual_content(self, content: str, platform: str) -> Dict:
        """Parse ChatGPT response into visual text and caption components"""
        lines = content.strip().split('\n')
        
        visual_text = ""
        caption = ""
        current_section = None
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Look for section headers
            if any(header in line.lower() for header in ['visual text:', 'on-screen text:', 'asset text:']):
                current_section = 'visual'
                continue
            elif any(header in line.lower() for header in ['caption:', 'description:', 'post caption:']):
                current_section = 'caption'
                continue
            
            # Add content to appropriate section
            if current_section == 'visual':
                visual_text += line + " "
            elif current_section == 'caption':
                caption += line + " "
            else:
                # If no clear section, treat first substantial line as visual, rest as caption
                if not visual_text and len(line) < 100:  # Short lines likely visual text
                    visual_text = line
                else:
                    caption += line + " "
        
        return {
            'visual_text': visual_text.strip(),
            'caption': caption.strip(),
            'platform': platform
        }
    
    def _get_system_prompt(self, platform: str) -> str:
        """Get platform-specific system prompt"""
        base_prompt = """You are a Gen Z social media expert creating viral content for live events and entertainment. 
        Your content should be data-driven but never boring, optimized for discovery, and designed to make people stop scrolling.
        
        CRITICAL RULES:
        1. NEVER share actual dollar amounts or GMS numbers - use relative terms like "massive surge" or "top performer"
        2. Always provide TWO separate outputs: VISUAL TEXT and CAPTION
        3. Write like Gen Z (but not cringe) - authentic, direct, no millennial energy
        4. Front-load artist/team names for SEO and discovery"""
        
        platform_specific = {
            'instagram': """For Instagram:
            - VISUAL TEXT: Punchy, data-forward, shareable. Think billboard text - immediate impact, no context needed
            - CAPTION: Keyword-optimized, artist name first, context for fans, discovery-friendly hashtags
            - Make it something fans want to repost to their Stories with their own reaction""",
            
            'tiktok': """For TikTok:
            - VISUAL TEXT: Hook them in 3 seconds. Bold claims, clear data points, fandom-specific language when relevant
            - CAPTION: Artist/team name upfront, trending keywords, context that drives engagement
            - Think viral potential - what would make someone duet or stitch this?""",
            
            'twitter': """For Twitter:
            - VISUAL TEXT: Tweet-length, concise but impactful
            - CAPTION: Extended context, hashtags, threading potential"""
        }
        
        return f"{base_prompt}\n\n{platform_specific.get(platform, platform_specific['instagram'])}"
    
    def _create_major_spike_template(self, event_data: Dict, platform: str) -> str:
        """Template for major performance spikes (5x+ career average)"""
        career_multiple = event_data.get('career_context', {}).get('vs_career_avg_multiple', 0)
        artist = event_data['classified_artist_name']
        event_name = event_data['event_name']
        location = f"{event_data['venue_city']}, {event_data['venue_country']}"
        intl_pct = event_data['international_pct']
        
        # Get fandom-specific context if available
        genre = event_data['genre'].lower()
        fandom_context = ""
        if 'hip hop' in genre or 'rap' in genre:
            fandom_context = "Consider adding hip-hop culture references if relevant"
        elif 'rock' in genre:
            fandom_context = "Consider rock/metal culture references if relevant"
        elif 'country' in genre:
            fandom_context = "Consider country music culture references if relevant"
        
        return f"""
Create viral {platform} content about this MASSIVE performance spike. Remember: NO dollar amounts!

EVENT: {artist} - {event_name} in {location}
KEY INSIGHT: Performing {career_multiple:.1f}x above career average - this is HUGE
SUPPORTING DATA: {intl_pct:.0f}% international buyers, #{event_data['rank']} trending this week

{fandom_context}

VISUAL TEXT (for the asset):
- Keep it under 15 words max
- Lead with the shocking stat
- Make it instantly shareable 
- No context needed - pure data impact
- Example style: "{artist} {location} show BREAKS CAREER RECORDS"

CAPTION (for discovery):
- Start with "{artist}" for SEO
- Include city/venue names early
- Add context about why this matters
- Use terms like "demand surge" instead of dollar amounts
- End with engaging question or call-out
- Include relevant hashtags

Make it feel like breaking news that fans need to share immediately.
"""

    def _create_international_phenomenon_template(self, event_data: Dict, platform: str) -> str:
        """Template for events with exceptional international appeal"""
        intl_pct = event_data['international_pct']
        artist = event_data['classified_artist_name']
        location = f"{event_data['venue_city']}, {event_data['venue_country']}"
        
        top_countries = event_data.get('geographic_insights', {}).get('top_buyer_countries', [])
        countries_list = [c['country'] for c in top_countries[:3]]
        
        return f"""
Create viral {platform} content about this event's INSANE international pull. NO dollar amounts!

EVENT: {artist} in {location}
KEY INSIGHT: {intl_pct:.0f}% international buyers - people are FLYING IN for this
TOP COUNTRIES: {', '.join(countries_list)}

VISUAL TEXT (for the asset):
- Under 15 words
- Lead with the shocking percentage
- Make the travel angle clear
- Example: "{intl_pct:.0f}% of {artist} {location} fans FLEW IN from other countries"

CAPTION (for discovery):
- Start with "{artist}" 
- Emphasize the global travel story
- Name specific countries
- Use phrases like "global phenomenon" or "international demand"
- Connect to the artist's worldwide appeal
- Ask fans about their concert travel stories
- Include location and travel-related hashtags

Make it feel like this artist is causing a worldwide movement.
"""

    def _create_genre_leader_template(self, event_data: Dict, platform: str) -> str:
        """Template for genre-leading performances"""
        genre_rank = event_data.get('market_position', {}).get('ytd_genre_rank', 999)
        genre = event_data['genre']
        artist = event_data['classified_artist_name']
        overall_rank = event_data.get('market_position', {}).get('ytd_overall_rank', 999)
        
        return f"""
Create viral {platform} content celebrating this artist OWNING their genre. NO dollar amounts!

EVENT: {artist} dominating {genre}
KEY INSIGHT: #{genre_rank} in {genre} this year, #{overall_rank} overall
IMPACT: Setting the standard for the entire genre

VISUAL TEXT (for the asset):
- Under 15 words
- Lead with their dominance
- Make the achievement clear
- Example: "{artist} is the #{genre_rank} {genre} artist of 2025"

CAPTION (for discovery):
- Start with "{artist}"
- Emphasize genre leadership
- Use power words: "dominating," "crushing," "leading"
- Compare to other artists in the genre
- Celebrate the fanbase
- Ask fans to show their support
- Include genre and achievement hashtags

Make fans feel proud to stan this artist - they're witnessing greatness.
"""

    def _create_international_phenomenon_template(self, event_data: Dict, platform: str) -> str:
        """Template for events with exceptional international appeal"""
        intl_pct = event_data['international_pct']
        artist = event_data['classified_artist_name']
        location = f"{event_data['venue_city']}, {event_data['venue_country']}"
        
        top_countries = event_data.get('geographic_insights', {}).get('top_buyer_countries', [])
        countries_text = ", ".join([f"{c['country']} ({c['percentage']:.0f}%)" for c in top_countries[:3]])
        
        return f"""
Create an engaging post about this event's incredible international draw:

🌍 GLOBAL PHENOMENON: {intl_pct:.0f}% of buyers for {artist} in {location} are traveling internationally!

TOP BUYER COUNTRIES: {countries_text}

ADDITIONAL CONTEXT:
- Total recent sales: ${event_data['recent_7d_gms']:,.0f}
- Genre: {event_data['genre']}
- Career performance: {event_data.get('career_context', {}).get('vs_career_avg_multiple', 1):.1f}x above average

Create a post that:
1. Emphasizes the global/travel angle (use 🌍✈️ emojis)
2. Highlights specific countries and percentages
3. Creates amazement at the international reach
4. Suggests this shows the artist's global appeal
5. Asks followers about their travel experiences for shows

Make it feel like a testament to the artist's worldwide fanbase.
"""

    def _create_genre_leader_template(self, event_data: Dict, platform: str) -> str:
        """Template for genre-leading performances"""
        genre_rank = event_data.get('market_position', {}).get('ytd_genre_rank', 999)
        genre = event_data['genre']
        artist = event_data['classified_artist_name']
        overall_rank = event_data.get('market_position', {}).get('ytd_overall_rank', 999)
        
        return f"""
Create a celebration post about this genre leadership achievement:

👑 GENRE LEADER: {artist} is #{genre_rank} in {genre} this year!

DOMINATION STATS:
- Overall market rank: #{overall_rank}
- Recent 7-day sales: ${event_data['recent_7d_gms']:,.0f}
- vs Genre average: {event_data.get('genre_context', {}).get('vs_genre_avg_multiple', 1):.1f}x above typical
- Market share: {event_data.get('market_position', {}).get('last_7d_market_share_pct', 0):.2f}%

Create a post that:
1. Celebrates the leadership position (use 👑🏆 emojis)
2. Puts the ranking in context (genre dominance)
3. Uses power words like "crushing," "dominating," "leading"
4. Shows the numbers that prove market leadership
5. Invites fans to celebrate the achievement

Make it feel like a victory lap that fans would want to share.
"""

    def _create_pricing_surge_template(self, event_data: Dict, platform: str) -> str:
        """Template for significant price appreciation events"""
        price_appreciation = event_data.get('trend_insights', {}).get('price_appreciation_pct', 0)
        artist = event_data['classified_artist_name']
        avg_cost = event_data['avg_ticket_cost']
        
        return f"""
Create an insightful post about this pricing surge:

📈 DEMAND INDICATOR: Ticket prices for {artist} have surged {price_appreciation:.0f}% in recent weeks!

MARKET SIGNALS:
- Current average ticket: ${avg_cost:,.0f}
- Recent sales momentum: ${event_data['recent_7d_gms']:,.0f}
- International demand: {event_data['international_pct']:.0f}%
- vs Career average: {event_data.get('career_context', {}).get('vs_career_avg_multiple', 1):.1f}x above typical

Create a post that:
1. Frames price increases as demand validation (use 📈💰 emojis)
2. Explains what this signals about fan enthusiasm
3. Connects pricing to broader success metrics
4. Avoids being too sales-y or promotional
5. Educates about market dynamics

Make it feel like valuable market insight that reveals the story behind the numbers.
"""

    def _create_tour_standout_template(self, event_data: Dict, platform: str) -> str:
        """Template for events that stand out within their tour"""
        tour_name = event_data.get('tour_context', {}).get('tour_name', 'Current Tour')
        tour_multiple = event_data.get('tour_context', {}).get('vs_tour_avg_multiple', 1)
        artist = event_data['classified_artist_name']
        location = f"{event_data['venue_city']}, {event_data['venue_country']}"
        
        return f"""
Create an exciting post about this tour standout performance:

⭐ TOUR STANDOUT: {artist}'s {location} show is {tour_multiple:.1f}x above their {tour_name} average!

WHY THIS STOP IS SPECIAL:
- Recent sales: ${event_data['recent_7d_gms']:,.0f}
- Tour performance: {tour_multiple:.1f}x above other stops
- International appeal: {event_data['international_pct']:.0f}% international buyers
- Market rank: #{event_data['rank']} this week

Create a post that:
1. Highlights what makes this tour stop special (use ⭐🎯 emojis)
2. Compares to other tour performances
3. Speculates on why this location is performing so well
4. Creates excitement for the tour
5. Asks fans about their favorite tour stops

Make it feel like insider knowledge about tour dynamics.
"""

    def _create_default_template(self, event_data: Dict, platform: str) -> str:
        """Default template for general strong performance"""
        artist = event_data['classified_artist_name']
        event_name = event_data['event_name']
        location = f"{event_data['venue_city']}, {event_data['venue_country']}"
        
        return f"""
Create an engaging post highlighting this event's strong performance:

🎵 TRENDING: {artist} - {event_name} in {location}

PERFORMANCE HIGHLIGHTS:
- Ranked #{event_data['rank']} in last 7 days
- Recent sales: ${event_data['recent_7d_gms']:,.0f}
- vs Career average: {event_data.get('career_context', {}).get('vs_career_avg_multiple', 1):.1f}x above typical
- International interest: {event_data['international_pct']:.0f}%
- Genre: {event_data['genre']}

Create a post that:
1. Highlights what makes this event notable
2. Uses specific metrics for credibility
3. Appeals to both fans and industry watchers
4. Creates interest without overhyping
5. Includes a relevant question for engagement

Keep it informative but exciting - think "industry insider sharing cool data."
"""

    def _create_significant_spike_template(self, event_data: Dict, platform: str) -> str:
        """Template for significant spikes (3-5x career average)"""
        return self._create_major_spike_template(event_data, platform).replace("MASSIVE", "SIGNIFICANT").replace("🔥", "📈")

    def _create_notable_performance_template(self, event_data: Dict, platform: str) -> str:
        """Template for notable performances (2-3x career average)"""
        return self._create_major_spike_template(event_data, platform).replace("MASSIVE", "NOTABLE").replace("🔥", "⚡")

    def _create_international_appeal_template(self, event_data: Dict, platform: str) -> str:
        """Template for moderate international appeal"""
        return self._create_international_phenomenon_template(event_data, platform).replace("PHENOMENON", "APPEAL").replace("incredible", "strong")

    def _create_top_performer_template(self, event_data: Dict, platform: str) -> str:
        """Template for top 10 genre performers"""
        return self._create_genre_leader_template(event_data, platform).replace("LEADER", "TOP PERFORMER").replace("👑", "🏆")

    def _create_demand_indicator_template(self, event_data: Dict, platform: str) -> str:
        """Template for moderate price appreciation"""
        return self._create_pricing_surge_template(event_data, platform).replace("surged", "increased").replace("📈", "📊")

    def _create_top_performance_template(self, event_data: Dict, platform: str) -> str:
        """Template for general top 5 performances"""
        return self._create_default_template(event_data, platform).replace("TRENDING", "TOP PERFORMANCE").replace("🎵", "🏆")

    def _create_trending_event_template(self, event_data: Dict, platform: str) -> str:
        """Template for trending events (default)"""
        return self._create_default_template(event_data, platform)