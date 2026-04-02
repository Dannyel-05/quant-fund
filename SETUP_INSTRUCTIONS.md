# Quant Fund Setup Instructions

## API Keys

Add all keys to `config/settings.yaml` under `api_keys:`.

| Key | Purpose | Where to get | Cost |
|-----|---------|--------------|------|
| `finnhub` | Company news, analyst ratings, earnings, insider tx | finnhub.io → Register → API Key | Free tier: 60 calls/min |
| `fred` | Macro economic data (rates, VIX, yield curve) | fred.stlouisfed.org → My Account → API Keys | Free |
| `alpha_vantage` | Earnings history, fundamentals | alphavantage.co → Get Free API Key | Free tier: 25 calls/day |
| `marketstack` | End-of-day price data backup | marketstack.com | Free tier: 100 calls/mo |
| `news_api` | Full article text from 80,000 sources | newsapi.org → Register | Free tier: 100 calls/day |
| `nasdaq_data_link` | Alternative data, commodities | data.nasdaq.com | Free tier available |
| `reddit_client_id` | Reddit WSB/investing sentiment | reddit.com/prefs/apps → Create App | Free |
| `reddit_client_secret` | Reddit authentication | Same as above | Free |
| `companies_house` | UK company filings | api.company-information.service.gov.uk | Free |
| `polygon` | Real-time US market data | polygon.io | Free tier available |

## Telegram Notifications

1. Open Telegram, search @BotFather
2. Send /newbot, follow prompts
3. Copy the bot token
4. Send your bot any message
5. Visit: `https://api.telegram.org/bot{TOKEN}/getUpdates`
6. Find `chat.id` in the response
7. Add to config/settings.yaml:
   ```yaml
   notifications:
     telegram:
       enabled: true
       bot_token: "YOUR_TOKEN_HERE"
       chat_id: "YOUR_CHAT_ID_HERE"
   ```

## Which Collectors Each Key Unlocks

- **No keys needed (working now)**: SEC EDGAR, FRED (yfinance fallback), yfinance prices, congressional disclosures
- **finnhub key unlocks**: Company news, analyst ratings, price targets, earnings calendar (best source)
- **fred key unlocks**: Full FRED macro data at higher rate limits
- **alpha_vantage key unlocks**: Deeper earnings history backup
- **news_api key unlocks**: Full article text from 80,000+ news sources
- **reddit keys unlock**: WSB/investing sentiment scoring
- **companies_house key unlocks**: UK company filings monitoring
