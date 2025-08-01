# 🎵 Social Content Generator

A Streamlit application that generates viral social media content for entertainment events using AI. Integrates with Snowflake data warehouse and OpenAI GPT-4o to create data-driven, engaging social media posts.

## ✨ Features

- **Data Integration**: Connects to Snowflake views for event data, trends, and analytics
- **AI Content Generation**: Uses OpenAI GPT-4o to create platform-specific social content
- **Multi-Platform Support**: Generates content optimized for Instagram, TikTok, and Twitter
- **Interactive UI**: Streamlit interface with real-time content generation and editing
- **Comprehensive Analytics**: Leverages event performance, international appeal, and trend data
- **Deployment Ready**: Supports both local development and Snowflake Streamlit deployment

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- OpenAI API key with GPT-4o access
- Snowflake account with required data views

### Local Development Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/maggie-li-sh/social-content-engine.git
   cd social-content-engine
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment**
   ```bash
   cp .env.example .env
   # Edit .env with your actual API keys and settings
   ```

4. **Run the application**
   ```bash
   streamlit run streamlit_app.py
   ```

### Snowflake Streamlit Deployment

1. **Prepare files**: Upload all Python files to your Snowflake Streamlit app
2. **Configure secrets**: Add `OPENAI_API_KEY` and `OPENAI_MODEL` to Snowflake secrets
3. **Deploy**: Launch your Streamlit app in Snowflake

For detailed deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).

## 📊 Required Snowflake Views

The application expects these views in your Snowflake database:

- `base_events` - Core event information
- `historical_context` - Performance trends and comparisons  
- `trend_analysis` - Market trends and insights
- `market_rankings` - Ranking and positioning data

## 🔧 Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

- `OPENAI_API_KEY` - Your OpenAI API key
- `OPENAI_MODEL` - Model to use (default: gpt-4o)
- Snowflake connection settings (optional for external connections)

### Snowflake Secrets

For Snowflake deployment, add these secrets:

```toml
OPENAI_API_KEY = "sk-proj-your-actual-api-key"
OPENAI_MODEL = "gpt-4o"
```

## 🎯 Usage

1. **Load Data**: Connect to Snowflake and load event data
2. **Select Events**: Choose events from the dropdown
3. **Edit Prompts**: Customize content generation prompts
4. **Generate Content**: Create social media posts with AI
5. **Review & Export**: View generated content and export as needed

## 🛠️ Development

### File Structure

```
├── streamlit_app.py           # Main Streamlit application
├── ai_contextualizer.py       # OpenAI integration and content generation
├── data_processing.py         # Snowflake data connection and processing
├── social_content_generator.py # Content pipeline orchestration
├── batch_processor.py         # Batch processing utilities
├── requirements.txt           # Python dependencies
├── .streamlit/
│   └── secrets.toml.example   # Snowflake secrets template
└── config/
    ├── settings.py            # Application settings
    └── styles.css             # Custom CSS styles
```

### Key Components

- **ContentGenerator**: Handles OpenAI API integration and prompt engineering
- **SnowflakeConnector**: Manages database connections and queries
- **SocialContentPipeline**: Orchestrates the content generation workflow
- **BatchProcessor**: Handles bulk processing operations

## 🔍 Troubleshooting

### Common Issues

1. **OpenAI Connection Error**: Check API key configuration and network access
2. **Snowflake Connection Failed**: Verify database credentials and permissions
3. **Model Not Available**: Ensure your OpenAI plan supports the selected model

### Debug Mode

The application includes comprehensive debugging features:
- Connection testing for both Snowflake and OpenAI
- Detailed error messages with troubleshooting guidance
- Step-by-step diagnostic information

## 📝 License

This project is part of internal tooling for social media content generation.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

---

Built with ❤️ using Streamlit, OpenAI, and Snowflake