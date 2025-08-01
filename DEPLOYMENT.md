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

#### ❌ "Connection Error" when generating content
**Cause:** OpenAI API key not accessible
**Solution:** Check that `OPENAI_API_KEY` is set in Snowflake secrets

#### ❌ "Model 'gpt-4o' does not exist"
**Cause:** Your OpenAI account doesn't have access to GPT-4o
**Solution:** Use `gpt-4` or `gpt-3.5-turbo` in secrets:
```toml
OPENAI_MODEL = "gpt-4"
```

#### ❌ Network/Firewall Issues
**Cause:** Snowflake environment blocking external API calls
**Solution:** Contact your Snowflake admin about OpenAI API access

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

### If content generation still fails:

1. **Check the error message** in the generated content display
2. **Verify API key** has sufficient credits and permissions
3. **Try different model** (gpt-4, gpt-3.5-turbo) in secrets
4. **Test with minimal data** (1 event, 1 content angle)
5. **Contact support** if network restrictions are suspected

### Debug Steps:
1. Use "Test Connections" button in sidebar
2. Check browser console for JavaScript errors
3. Verify all secrets are properly set
4. Try regenerating content after clearing cache

## 📞 Support

If you continue to have issues:
1. Check Snowflake Streamlit documentation
2. Verify OpenAI API status and quotas
3. Test the app locally first to isolate deployment issues