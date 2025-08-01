# 🚀 Snowflake Streamlit Deployment Guide

## 📋 Prerequisites

1. **Snowflake Account** with Streamlit enabled
2. **OpenAI API Key** with access to GPT-4o model
3. **Required Permissions** for the Snowflake views

## 🔧 Setup Instructions

### 1. **Prepare Your Files**
```bash
# Ensure all files are in the root directory (flat structure)
streamlit_app.py
ai_contextualizer.py
batch_processor.py
data_processing.py
social_content_generator.py
requirements.txt
```

### 2. **Configure Secrets in Snowflake**

In your Snowflake Streamlit app settings, add these secrets:

```toml
# Required for OpenAI
OPENAI_API_KEY = "sk-proj-your-actual-api-key-here"
OPENAI_MODEL = "gpt-4o"

# Optional Snowflake overrides (usually auto-configured)
SNOWFLAKE_WAREHOUSE = "GENERAL_XS"
SNOWFLAKE_DATABASE = "ADHOC"
SNOWFLAKE_SCHEMA = "MAGGIELI"
```

### 3. **Update Requirements**
Make sure your `requirements.txt` includes:
```
streamlit
pandas
numpy
openai
python-dotenv
```

### 4. **Common Issues & Solutions**

#### ❌ "Network connection error" when generating content
**Cause:** Snowflake environment blocking external API calls to OpenAI
**Solution:** Contact your Snowflake administrator to whitelist `api.openai.com`

#### ❌ "Invalid API key" error
**Cause:** OpenAI API key not properly configured
**Solution:** Check that `OPENAI_API_KEY` is set in Snowflake secrets

#### ❌ "Model 'gpt-4o' does not exist"
**Cause:** Your OpenAI account doesn't have access to GPT-4o
**Solution:** Use `gpt-4` or `gpt-3.5-turbo` in secrets:
```toml
OPENAI_MODEL = "gpt-4"
```

#### ❌ "Module not found" errors
**Cause:** Missing dependencies
**Solution:** Ensure all required packages are in `requirements.txt`

### 5. **Testing the Deployment**

1. **Test Connections** - Use sidebar "Test Connections" button
2. **Load Data** - Go to "Data Preview" and load events
3. **Generate Content** - Try generating content for 1-2 events first
4. **Check Results** - Verify content appears properly

### 6. **Deployment Checklist**

- [ ] All Python files in root directory (no `src/` folder)
- [ ] `requirements.txt` includes `openai` package
- [ ] OpenAI API key added to Snowflake secrets
- [ ] OpenAI model specified in secrets (default: `gpt-4o`)
- [ ] Snowflake views accessible from the app environment
- [ ] Connection tests pass in sidebar
- [ ] Content generation works with test events

### 7. **Performance Tips**

- **Limit Events**: Start with 1-3 events to test
- **Use Caching**: Data is cached for 5 minutes automatically
- **Monitor Usage**: Track OpenAI API usage to avoid rate limits
- **Reset on Issues**: Use "Reset App" button to clear any stuck states

## 🆘 Troubleshooting

### If content generation fails:

1. **Network Issues**: Most common cause is Snowflake blocking external API calls
   - Contact your Snowflake administrator to whitelist `api.openai.com`
2. **API Key Issues**: Verify your OpenAI API key is properly configured in Snowflake secrets
3. **Model Access**: Try a different model (gpt-4, gpt-3.5-turbo) if GPT-4o is not available
4. **Test Connections**: Use the "Test Connections" button in the sidebar to verify setup

## 📞 Support

If you continue to have issues:
1. Check Snowflake Streamlit documentation
2. Verify OpenAI API status and quotas
3. Test the app locally first to isolate deployment issues