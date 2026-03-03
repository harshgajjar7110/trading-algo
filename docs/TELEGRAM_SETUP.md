# Telegram Notifications Setup Guide

This guide will help you set up Telegram notifications for your trading strategy.

## Overview

Telegram notifications provide real-time alerts for:
- ✅ Order placed/filled/rejected
- ✅ Position opened/closed
- ✅ Stop-loss triggered
- ✅ Strategy started/stopped
- ✅ Errors and warnings
- ✅ Daily summary reports

## Setup Instructions

### Step 1: Create a Telegram Bot

1. Open Telegram and search for **@BotFather**
2. Start a chat and send `/newbot`
3. Follow the prompts:
   - Enter a name for your bot (e.g., "My Trading Bot")
   - Enter a username (must end in 'bot', e.g., "my_trading_bot")
4. BotFather will give you a **token** - save this securely!

Example token: `123456789:ABCdefGHIjklMNOpqrsTUVwxyz`

### Step 2: Get Your Chat ID

**Option A: Using @userinfobot**
1. Search for **@userinfobot** in Telegram
2. Start the bot
3. It will reply with your user info including the **Id**

**Option B: Using your new bot**
1. Send a message to your new bot
2. Visit this URL in your browser:
   ```
   https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
   ```
3. Look for the `chat` object and find the `id` field

Example chat ID: `123456789` (for personal chats) or `-1234567890123` (for groups)

### Step 3: Configure Environment Variables

Add these to your `.env` file in the project root:

```bash
# Telegram Notifications
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=your_chat_id_here
```

**Example:**
```bash
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_CHAT_ID=123456789
```

### Step 4: Test the Setup

1. Restart the backend server:
   ```bash
   cd web/backend
   uvicorn app.main:app --reload
   ```

2. Check the logs - you should see:
   ```
   [TelegramNotifier] Initialized with chat ID: 123456789
   ```

3. Start your strategy - you should receive a Telegram message!

## Notification Types

### 🚀 Strategy Started
Sent when strategy begins trading
```
🚀 STRATEGY STARTED

Strategy: Survivor
Time: 2026-03-03 09:15:00
index_symbol: NSE:NIFTY 50
gap: 50
sl_percentage: 60

Trading session started. Good luck! 🎯
```

### 🛑 Strategy Stopped
Sent when strategy stops
```
🛑 STRATEGY STOPPED

Strategy: Survivor
Time: 2026-03-03 15:30:00
Reason: User initiated stop

Trading session ended.
```

### 🔴 Order Placed (SELL)
```
🔴 ORDER PLACED

Symbol: NIFTY26MAR22500CE
Type: SELL
Quantity: 50
Price: ₹50.00
Order ID: ORDER0001
Time: 2026-03-03 10:00:00
```

### 🟢 Order Placed (BUY)
```
🟢 ORDER PLACED

Symbol: NIFTY26MAR22500CE
Type: BUY
Quantity: 50
Price: ₹45.00
Order ID: ORDER0002
Time: 2026-03-03 11:30:00
```

### ✅ Order Filled
```
✅ ORDER FILLED

Symbol: NIFTY26MAR22500CE
Type: SELL
Quantity: 50
Fill Price: ₹50.50
Order ID: ORDER0001
Time: 2026-03-03 10:00:05
```

### ❌ Order Rejected
```
❌ ORDER REJECTED

Symbol: NIFTY26MAR22500CE
Order ID: ORDER0003
Reason: Insufficient funds
Time: 2026-03-03 10:05:00

Please check the strategy and broker configuration.
```

### 📉 Position Opened (Short)
```
📉 POSITION OPENED

Symbol: NIFTY26MAR22500CE
Side: SHORT
Quantity: 50
Average Price: ₹50.00
Time: 2026-03-03 10:00:05
```

### 🟢 Position Closed (Profit)
```
🟢 POSITION CLOSED

Symbol: NIFTY26MAR22500CE
Quantity: 50
P&L: +₹2,500.00
Time: 2026-03-03 14:30:00
```

### 🔴 Position Closed (Loss)
```
🔴 POSITION CLOSED

Symbol: NIFTY26MAR22500CE
Quantity: 50
P&L: -₹1,000.00
Time: 2026-03-03 12:15:00
```

### 🛑 Stop-Loss Triggered
```
🛑 STOP-LOSS TRIGGERED

Symbol: NIFTY26MAR22500CE
Stop-Loss Price: ₹80.00
Time: 2026-03-03 13:45:00

Position will be exited automatically.
```

### ⚠️ Strategy Error
```
⚠️ STRATEGY ERROR

Error: Connection timeout
Context: Strategy: Survivor
Time: 2026-03-03 11:20:00

Please check the logs for details.
```

### 📊 Daily Summary
```
📊 DAILY TRADING SUMMARY
2026-03-03

Total P&L: 🟢 +₹5,250.00
Positions: 4
Orders: 8
Wins: 3
Losses: 1
Win Rate: 75.0%

Keep trading! 📈
```

## Troubleshooting

### Notifications Not Working

1. **Check logs** for error messages:
   ```bash
   # Look for [TelegramNotifier] messages
   ```

2. **Verify environment variables**:
   ```bash
   # Check if variables are set
   echo $TELEGRAM_BOT_TOKEN
   echo $TELEGRAM_CHAT_ID
   ```

3. **Test bot token**:
   ```bash
   curl https://api.telegram.org/bot<YOUR_TOKEN>/getMe
   ```
   Should return bot info

4. **Test sending message**:
   ```bash
   curl -X POST \
     -H 'Content-Type: application/json' \
     -d '{"chat_id": "<YOUR_CHAT_ID>", "text": "Test message"}' \
     https://api.telegram.org/bot<YOUR_TOKEN>/sendMessage
   ```

### Common Issues

**Issue:** `[TelegramNotifier] Disabled - set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to enable`

**Solution:** Environment variables not set. Add them to `.env` file and restart server.

---

**Issue:** `[TelegramNotifier] Failed to send message: {"ok":false,"error_code":401,"description":"Unauthorized"}`

**Solution:** Invalid bot token. Get a new token from @BotFather.

---

**Issue:** `[TelegramNotifier] Failed to send message: {"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`

**Solution:** Invalid chat ID. Make sure you've sent a message to the bot first.

---

**Issue:** Messages not formatted properly

**Solution:** This is normal - Telegram uses HTML formatting which may look different in the API response but renders correctly in the app.

## Security Best Practices

1. **Never commit your bot token** to version control
2. **Keep your `.env` file private**
3. **Use a dedicated bot** for trading (don't reuse personal bots)
4. **Regularly rotate tokens** if needed
5. **Don't share your chat ID** publicly

## Customization

To customize notifications, edit:
- `web/backend/app/services/telegram_notifier.py`

You can modify:
- Message templates
- Emoji usage
- Information displayed
- Formatting (HTML/Markdown)

## Disabling Notifications

To disable notifications, either:
1. Remove/comment out the environment variables
2. Set `TELEGRAM_BOT_TOKEN=` (empty)

The system will log: `[TelegramNotifier] Disabled`

## Rate Limits

Telegram has rate limits:
- ~30 messages per second per bot
- This should not be an issue for normal trading

If you hit limits, the notifier will log errors but continue operating.

## Support

For issues with:
- **Telegram API**: https://core.telegram.org/bots/api
- **Bot creation**: Message @BotFather in Telegram
- **This integration**: Check the logs and this guide
