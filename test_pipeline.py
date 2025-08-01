#!/usr/bin/env python3
"""
Test script for Social Media Content Generation Pipeline
Run this to verify your setup and test the pipeline
"""

import os
from datetime import datetime
from dotenv import load_dotenv

# Files are now in root directory

def check_environment():
    """Check if all required environment variables are set"""
    print("🔍 Checking environment configuration...")
    
    load_dotenv()
    
    # Check if running locally
    is_local = os.getenv('IS_LOCAL_DEV', '') == '1'
    print(f"  🏠 Running locally: {is_local}")
    
    if is_local:
        required_vars = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER', 
            'SNOWFLAKE_AUTHENTICATOR',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_ROLE',
            'OPENAI_API_KEY'
        ]
    else:
        # In Snowflake environment, only need OpenAI key
        required_vars = ['OPENAI_API_KEY']
    
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
            print(f"  ❌ {var}: Not set")
        else:
            # Show first few chars for security
            if 'KEY' in var or 'PASSWORD' in var:
                display_value = value[:4] + "..." if len(value) > 4 else "***"
            else:
                display_value = value[:20] + "..." if len(value) > 20 else value
            print(f"  ✅ {var}: {display_value}")
    
    if missing_vars:
        print(f"\n❌ Missing environment variables: {', '.join(missing_vars)}")
        if is_local:
            print("Please add these to your .env file")
            print("Note: Use SNOWFLAKE_AUTHENTICATOR=externalbrowser for SSO")
        return False
    
    print("✅ All required environment variables are set")
    return True

def test_snowflake_connection():
    """Test Snowflake connection and view access"""
    print("\n🔌 Testing Snowflake connection...")
    
    try:
        from data_processing import SnowflakeConnector
        
        connector = SnowflakeConnector()
        
        # Test basic connection
        if not connector.test_connection():
            return False
        
        # Test view access
        print("📊 Checking view access...")
        view_status = connector.validate_views_exist()
        
        all_accessible = all(view_status.values())
        
        if all_accessible:
            print("✅ All views are accessible")
        else:
            print("⚠️  Some views are not accessible:")
            for view, accessible in view_status.items():
                status = "✅" if accessible else "❌"
                print(f"    {status} {view}")
        
        connector.close_connection()
        return all_accessible
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure all required packages are installed")
        return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def test_openai_connection():
    """Test OpenAI API connection"""
    print("\n🤖 Testing OpenAI API connection...")
    
    try:
        import openai
        
        client = openai.OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
        
        # Simple test call
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[{"role": "user", "content": "Test message - please respond with 'API working'"}],
            max_tokens=10
        )
        
        if "API working" in response.choices[0].message.content:
            print("✅ OpenAI API connection successful")
            return True
        else:
            print("⚠️  OpenAI API responded but with unexpected content")
            return True  # Still working, just different response
            
    except Exception as e:
        print(f"❌ OpenAI API test failed: {e}")
        return False

def run_sample_pipeline():
    """Run a small sample of the pipeline"""
    print("\n🚀 Running sample pipeline...")
    
    try:
        from social_content_generator import SocialContentPipeline
        
        pipeline = SocialContentPipeline()
        
        # Query just the base events view for testing
        print("📊 Querying sample data...")
        dataframes = pipeline.query_top_events_views()
        
        if dataframes['base_events'].empty:
            print("❌ No data returned from views")
            return False
        
        # Structure a few events
        events = pipeline.structure_event_data(dataframes)
        
        if not events:
            print("❌ No events processed")
            return False
        
        print(f"✅ Successfully processed {len(events)} events")
        
        # Show sample event data
        sample_event = events[0]
        print(f"\n📋 Sample event data:")
        print(f"  Artist: {sample_event['classified_artist_name']}")
        print(f"  Event: {sample_event['event_name']}")
        print(f"  Location: {sample_event['venue_city']}, {sample_event['venue_country']}")
        print(f"  Rank: #{sample_event['rank']}")
        print(f"  Recent GMS: ${sample_event['recent_7d_gms']:,.0f}")
        print(f"  Data completeness: {sample_event['data_completeness']['completeness_score']:.1%}")
        
        # Identify content angles
        angles = pipeline.identify_content_angles(sample_event)
        print(f"  Content angles: {', '.join(angles)}")
        
        print("\n✅ Sample pipeline test successful!")
        return True
        
    except Exception as e:
        print(f"❌ Sample pipeline failed: {e}")
        return False

def generate_test_content():
    """Generate one piece of test content"""
    print("\n✍️  Generating test content...")
    
    try:
        from social_content_generator import SocialContentPipeline
        
        pipeline = SocialContentPipeline()
        
        # Get data
        dataframes = pipeline.query_top_events_views()
        events = pipeline.structure_event_data(dataframes)
        
        if not events:
            print("❌ No events to test with")
            return False
        
        # Generate content for the top event
        test_event = events[0]
        angles = pipeline.identify_content_angles(test_event)
        
        if not angles:
            print("❌ No content angles identified")
            return False
        
        # Generate content
        content = pipeline.content_generator.create_social_post(
            event_data=test_event,
            content_angle=angles[0],
            platform='twitter'
        )
        
        print(f"✅ Generated test content:")
        print(f"   Event: {test_event['classified_artist_name']} - {test_event['event_name']}")
        print(f"   Angle: {angles[0]}")
        print(f"   Content: {content[:200]}...")
        
        return True
        
    except Exception as e:
        print(f"❌ Test content generation failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Social Media Content Pipeline - Setup Test")
    print("=" * 50)
    
    tests = [
        ("Environment Check", check_environment),
        ("Snowflake Connection", test_snowflake_connection),
        ("OpenAI API", test_openai_connection),
        ("Sample Pipeline", run_sample_pipeline),
        ("Test Content Generation", generate_test_content)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        try:
            results[test_name] = test_func()
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    print(f"\n{'='*50}")
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        print(f"  {status} {test_name}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your pipeline is ready to use.")
        print("\nNext steps:")
        print("  1. Run: python social_content_generator.py")
        print("  2. Check output in data/generated_content/")
        print("  3. Set up Zapier automation if desired")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please fix the issues above.")
        print("Check your .env file and network connections.")

if __name__ == "__main__":
    main()