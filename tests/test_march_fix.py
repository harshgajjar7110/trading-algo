"""Test March expiry parsing fix"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'web', 'backend'))

from app.services.greeks_calculator import BlackScholesGreeks

bs = BlackScholesGreeks()

# User's problematic symbols
test_symbols = [
    'NIFTY2630224750PE',   # March 22 weekly - was failing
    'NIFTY2630224800PE',   # March 22 weekly
    'NIFTY2630226500CE',   # March 22 weekly  
    'NIFTY26FEB24850PE',   # Feb monthly - was working
    'NIFTY26FEB1324000CE', # Feb 13 weekly
]

print('Testing March symbol parsing fix:')
print('=' * 60)

for sym in test_symbols:
    result = bs.parse_option_symbol(sym)
    if result:
        print(f'{sym}:')
        print(f"  Index: {result['index_name']}")
        print(f"  Expiry: {result['expiry_date']} ({result['expiry_type']})")
        print(f"  Strike: {result['strike']}")
        print(f"  Type: {result['option_type'].value}")
    else:
        print(f'{sym}: FAILED TO PARSE')
    print()

print('=' * 60)
print('All tests completed!')
