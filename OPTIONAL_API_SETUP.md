# Optional API Keys Setup Guide

These APIs extend the macro intelligence layer with physical, geopolitical, and environmental data.
All are free-tier or free to obtain. Add keys to `config/settings.yaml` under `api_keys:`.

---

## EIA — US Energy Information Administration
**What it gives you:** Real-time US electricity demand by region (PJM, MISO, CAISO, ERCOT, etc.)
Used as a proxy for industrial economic activity.

**Status:** ✅ Key already configured

**How to get a new key:**
1. Go to: https://www.eia.gov/opendata/register.php
2. Enter your email address — key emailed within minutes
3. Free, no credit card

**Test:** `curl "https://api.eia.gov/v2/electricity/rto/region-data/data/?api_key=YOUR_KEY&length=1"`

---

## OpenWeatherMap
**What it gives you:** Real-time current weather + 5-day forecast for all 20 tracked cities.
Supplements Open-Meteo historical with live readings.

**Status:** ✅ Key already configured

**How to get a new key:**
1. Register at: https://openweathermap.org/api
2. Free tier: 60 calls/minute, 1 million calls/month
3. Key active within 2 hours of registration

**Test:** `curl "https://api.openweathermap.org/data/2.5/weather?q=London&appid=YOUR_KEY"`

---

## WAQI — World Air Quality Index
**What it gives you:** PM2.5, PM10, O3, NO2 readings for major cities.
Used alongside pollen data for urban air quality stress signals.

**Status:** ✅ Key already configured

**How to get a new key:**
1. Request at: https://aqicn.org/data-platform/token/
2. Free for non-commercial use
3. Key active immediately

**Test:** `curl "https://api.waqi.info/feed/london/?token=YOUR_KEY"`

---

## ESA Copernicus Data Space
**What it gives you:** Sentinel-2 satellite imagery (10m resolution, every 10 days).
Used for parking lot car counting (retail activity proxy) and port ship counting.

**Status:** ✅ Credentials already configured

**How to get new credentials:**
1. Register at: https://dataspace.copernicus.eu
2. Free account — no credit card
3. Use email + password directly (OAuth2 token auth)

**Python usage:**
```python
import requests
token_r = requests.post(
    'https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
    data={'grant_type': 'password', 'username': EMAIL, 'password': PASSWORD, 'client_id': 'cdse-public'}
)
access_token = token_r.json()['access_token']
```

---

## ACLED — Armed Conflict Location and Event Data
**What it gives you:** Precise armed conflict events globally back to 1997.
More accurate than GDELT for war/conflict signal generation.

**Status:** No key configured (optional)

**How to get a key:**
1. Register at: https://acleddata.com/register/
2. Free for academic/research use
3. Submit a brief description of your use case
4. Key emailed within 1-2 business days

**Add to config/settings.yaml under api_keys:**
```yaml
acled: "YOUR_KEY_HERE"
acled_email: "your@email.com"
```

**Test:** `curl "https://api.acleddata.com/acled/read?key=KEY&email=EMAIL&limit=1"`

---

## Reddit API (for social sentiment)
**What it gives you:** Reddit posts from r/wallstreetbets, r/stocks, r/investing, r/UKInvesting.
Sentiment analysis on retail investor mood.

**Status:** No key configured (falls back to public JSON API)

**How to get keys:**
1. Go to: https://www.reddit.com/prefs/apps
2. Click "Create another app"
3. Select "script" type
4. Note the `client_id` (below app name) and `client_secret`

**Add to config/settings.yaml under api_keys:**
```yaml
reddit_client_id: "YOUR_CLIENT_ID"
reddit_client_secret: "YOUR_CLIENT_SECRET"
```

---

## UK Companies House API
**What it gives you:** UK company filing changes (director changes, accounts, ownership).
Used for insider activity signals on UK small-caps.

**Status:** No key configured (optional)

**How to get a key:**
1. Register at: https://developer.company-information.service.gov.uk
2. Free, unlimited for personal/research use
3. Key generated immediately

**Add to config/settings.yaml under api_keys:**
```yaml
companies_house: "YOUR_KEY_HERE"
```

---

## Alpha Vantage
**What it gives you:** Stock quotes, technical indicators, fundamental data.
Currently rate-limited at 25 calls/day (free tier).

**Status:** ⚠️ Rate limited (key valid, 25 requests/day on free tier)

**Free tier limitations:**
- 25 API requests per day
- No intraday data on free tier
- Premium starts at $50/month for 75 req/min

**How to get a new free key:**
1. Go to: https://www.alphavantage.co/support/#api-key
2. Enter your email — key shown immediately
3. No credit card required

---

## FRED (Federal Reserve Economic Data)
**Status:** ✅ Key configured and working

**Current key limits:** ~120 requests/minute (free with key)
**Without key:** 10 requests/minute

**Free key from:** https://fred.stlouisfed.org/docs/api/api_key.html

---

## Nasdaq Data Link (formerly Quandl)
**Status:** ✅ Key configured (bot protection on historical endpoints, API works)

**Free tier gives you:**
- All FRED mirror datasets
- Some equity data
- Premium datasets require subscription

**Free key from:** https://data.nasdaq.com/sign-up

---

## Summary Table

| API | Status | Cost | Use Case |
|-----|--------|------|----------|
| finnhub | ✅ Working | Free | Stock quotes, earnings calendar |
| fred | ✅ Working | Free | All macro economic series |
| news_api | ✅ Working | Free (100 req/day) | News monitoring |
| marketstack | ✅ Working | Free (100 req/mo) | Historical prices |
| nasdaq_data_link | ✅ Working | Free tier | Economic datasets |
| eia | ✅ Working | Free | US electricity demand |
| openweathermap | ✅ Working | Free | Weather current + forecast |
| waqi | ✅ Working | Free | Air quality data |
| esa_copernicus | ✅ Working | Free | Satellite imagery |
| alpha_vantage | ⚠️ Rate limited | Free (25/day) | Supplementary quotes |
| acled | ❌ Not configured | Free (academic) | Conflict event data |
| reddit | ❌ Not configured | Free | Social sentiment |
| companies_house | ❌ Not configured | Free | UK filing changes |

---

## Telegram Bot Setup

Enables real-time signal alerts and trade notifications on your phone via Telegram.

1. Open Telegram on your phone
2. Search for @BotFather
3. Send: /newbot
4. Follow prompts to name your bot
5. BotFather gives you a token like: 1234567890:ABCdefGHIjkl...
6. Get your chat ID:
   - Message your new bot once (say /start)
   - Visit: https://api.telegram.org/bot{YOUR_TOKEN}/getUpdates
   - Find "chat" : {"id" : 123456789}
7. Add to config/settings.yaml:
   ```yaml
   notifications:
     telegram:
       enabled: true
       bot_token: "your_token_here"
       chat_id: "your_chat_id_here"
   ```

**Notification types sent:**
- Signal alerts (LONG/SHORT with surprise %, confidence, MFS)
- Trade close notifications (return %, holding days, paper P&L)
- Daily portfolio summaries (regime, VIX, open positions, P&L)
- Risk alerts (drawdown halt, source failures, model rollbacks)
