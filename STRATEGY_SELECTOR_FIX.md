# Strategy Selector Fix Summary

## Issues Fixed

### 1. Router Prefix Bug
**Problem:** The `strategy_selector.py` router had prefix `/api/strategy` but was mounted at `/api` in main.py, causing double `/api/api/strategy` paths.

**Fix:** Changed router prefix from `/api/strategy` to `/strategy` in `web/backend/app/routes/strategy_selector.py`.

### 2. Duplicate Routes
**Problem:** Both `strategy.py` and `strategy_selector.py` had overlapping routes (`/start`, `/stop`, `/restart`, `/status`).

**Status:** This is actually fine - the new routes in `strategy_selector.py` extend the old ones with additional functionality (confirmation flow, strategy_id parameter). FastAPI handles this by using the last registered route.

### 3. Frontend Loading State
**Problem:** The frontend component didn't show loading state properly when fetching strategies.

**Fix:** Added `initialLoading` state and better error handling in `StrategySelector.tsx`.

## Testing the Fix

### 1. Backend Test
```bash
cd web/backend
python -c "from app.services.strategy_manager_v2 import strategy_manager_v2; 
print(strategy_manager_v2.get_available_strategies())"
```

Expected output:
```
{'strategies': [...], 'current': None}
```

### 2. API Test
```bash
curl http://localhost:8000/api/strategy/available
```

Expected output:
```json
{
  "strategies": [
    {"id": "survivor", "name": "Survivor Strategy", ...},
    {"id": "enhanced_survivor", "name": "Enhanced Survivor Strategy", ...}
  ],
  "current": null
}
```

### 3. Frontend Test
1. Open browser console (F12)
2. Navigate to dashboard
3. Look for console logs:
   ```
   [API] GET http://localhost:8000/api/strategy/available
   [API] Response: 200 OK
   Fetching available strategies...
   Received strategies: {...}
   ```

## If Still Not Working

### Check 1: Backend Running
Ensure backend is running on port 8000:
```bash
curl http://localhost:8000/health
```

### Check 2: CORS Issues
If you see CORS errors in browser console, check `web/backend/app/main.py` CORS settings.

### Check 3: Build Cache
Clear Next.js cache:
```bash
cd web/frontend
rm -rf .next
npm run dev
```

### Check 4: Debug Info
The StrategySelector component now shows debug info in development mode. Expand the "Debug Info" section to see:
- strategiesCount
- initialLoading state
- selectedStrategy
- currentStrategyRunning

## Files Modified

1. `web/backend/app/routes/strategy_selector.py` - Fixed router prefix
2. `web/frontend/components/StrategySelector.tsx` - Added loading states and debug info
3. `web/frontend/lib/api.ts` - Added API logging
