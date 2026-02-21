"""
Test script for Greeks calculator symbol parsing

Tests various NIFTY option symbol formats to ensure correct parsing.
"""

import sys
sys.path.insert(0, 'web/backend')

from app.services.greeks_calculator import BlackScholesGreeks

def test_symbol_parsing():
    """Test parsing of various option symbol formats"""
    
    test_cases = [
        # (symbol, expected_index, expected_expiry_type, expected_strike)
        # Monthly formats
        ("NIFTY24FEB24000CE", "NIFTY", "MONTHLY", 24000),
        ("NIFTY24FEB24000PE", "NIFTY", "MONTHLY", 24000),
        ("NIFTY25MAR25000CE", "NIFTY", "MONTHLY", 25000),
        
        # Weekly formats with TEXT month (MMM + DD)
        ("NIFTY24FEB1324000CE", "NIFTY", "WEEKLY", 24000),  # Feb 13 weekly
        ("NIFTY24FEB0624000PE", "NIFTY", "WEEKLY", 24000),  # Feb 6 weekly
        ("NIFTY24FEB2024000CE", "NIFTY", "WEEKLY", 24000),  # Feb 20 weekly
        
        # Weekly formats with NUMERIC date (MDD - 3 digits for months 1-9)
        # Format: INDEX + YY + MDD + STRIKE where MDD = Month (1 digit) + Day (2 digits)
        ("NIFTY2630224750PE", "NIFTY", "WEEKLY", 24750),  # March 02 weekly (302)
        ("NIFTY2631924000CE", "NIFTY", "WEEKLY", 24000),  # March 19 weekly (319)
        ("NIFTY2640524500PE", "NIFTY", "WEEKLY", 24500),  # April 05 weekly (405)
        
        # BANKNIFTY
        ("BANKNIFTY24FEB45000CE", "BANKNIFTY", "MONTHLY", 45000),
        ("BANKNIFTY24FEB1345000PE", "BANKNIFTY", "WEEKLY", 45000),
        
        # FINNIFTY
        ("FINNIFTY24FEB23000CE", "FINNIFTY", "MONTHLY", 23000),
        ("FINNIFTY24FEB1323000PE", "FINNIFTY", "WEEKLY", 23000),
        
        # MIDCPNIFTY
        ("MIDCPNIFTY24FEB12000CE", "MIDCPNIFTY", "MONTHLY", 12000),
        
        # 2-letter month variants
        ("NIFTY24FE24000CE", "NIFTY", "MONTHLY", 24000),
        ("NIFTY24JA24000PE", "NIFTY", "MONTHLY", 24000),
    ]
    
    print("=" * 80)
    print("Testing Option Symbol Parsing")
    print("=" * 80)
    
    bs = BlackScholesGreeks()
    passed = 0
    failed = 0
    
    for symbol, expected_index, expected_type, expected_strike in test_cases:
        result = bs.parse_option_symbol(symbol)
        
        if result is None:
            print(f"[FAIL] FAILED: {symbol}")
            print(f"   Could not parse symbol")
            failed += 1
            continue
        
        errors = []
        if result.get('index_name') != expected_index:
            errors.append(f"index: got {result.get('index_name')}, expected {expected_index}")
        if result.get('expiry_type') != expected_type:
            errors.append(f"type: got {result.get('expiry_type')}, expected {expected_type}")
        if result.get('strike') != expected_strike:
            errors.append(f"strike: got {result.get('strike')}, expected {expected_strike}")
        
        if errors:
            print(f"❌ FAILED: {symbol}")
            for e in errors:
                print(f"   {e}")
            print(f"   Full result: {result}")
            failed += 1
        else:
            print(f"[PASS] PASSED: {symbol}")
            print(f"   Index: {result['index_name']}, Type: {result['expiry_type']}, "
                  f"Strike: {result['strike']}, Expiry: {result.get('expiry_date', 'N/A')}")
            passed += 1
    
    print("=" * 80)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 80)
    
    if failed == 0:
        print("All tests passed!")
    else:
        print("Some tests failed!")
    
    return failed == 0


def test_expiry_dates():
    """Test that expiry dates are calculated correctly"""
    
    print("\n" + "=" * 80)
    print("Testing Expiry Date Calculation")
    print("=" * 80)
    
    bs = BlackScholesGreeks()
    
    # Test monthly expiry (last Thursday)
    monthly_cases = [
        ("NIFTY24FEB24000CE", 2024, 2, 29),  # Last Thursday of Feb 2024
        ("NIFTY24MAR24000CE", 2024, 3, 28),  # Last Thursday of Mar 2024
        ("NIFTY25JAN24000CE", 2025, 1, 30),  # Last Thursday of Jan 2025
    ]
    
    for symbol, exp_year, exp_month, exp_day in monthly_cases:
        result = bs.parse_option_symbol(symbol)
        if result:
            expiry = result['expiry_date']
            expected = f"{exp_year}-{exp_month:02d}-{exp_day:02d}"
            actual = expiry.isoformat()
            
            if expiry.year == exp_year and expiry.month == exp_month and expiry.day == exp_day:
                print(f"[OK] {symbol}: Expiry {actual} (correct)")
            else:
                print(f"[ERR] {symbol}: Expiry {actual}, expected {expected}")
        else:
            print(f"❌ {symbol}: Could not parse")
    
    # Test weekly expiry (specific day) - NIFTY weekly options expire on Thursdays
    # The day in the symbol is the Thursday expiry date
    weekly_cases = [
        ("NIFTY24FEB1524000CE", 2024, 2, 15),  # Feb 15 is a Thursday
        ("NIFTY24FEB0824000CE", 2024, 2, 8),   # Feb 8 is a Thursday
    ]
    
    print("\nWeekly expiries:")
    for symbol, exp_year, exp_month, exp_day in weekly_cases:
        result = bs.parse_option_symbol(symbol)
        if result:
            expiry = result['expiry_date']
            expected = f"{exp_year}-{exp_month:02d}-{exp_day:02d}"
            actual = expiry.isoformat()
            
            if expiry.year == exp_year and expiry.month == exp_month and expiry.day == exp_day:
                print(f"[OK] {symbol}: Expiry {actual} (correct)")
            else:
                print(f"[ERR] {symbol}: Expiry {actual}, expected {expected}")
        else:
            print(f"❌ {symbol}: Could not parse")


def test_portfolio_with_multiple_expiries():
    """Test portfolio calculation with multiple expiry dates"""
    
    print("\n" + "=" * 80)
    print("Testing Portfolio with Multiple Expiries")
    print("=" * 80)
    
    from app.services.greeks_calculator import PortfolioGreeksCalculator
    
    # Simulate positions across different expiries
    positions = [
        {'symbol': 'NIFTY24FEB2024000CE', 'quantity': -50, 'average_price': 150.0},  # Weekly Feb 20
        {'symbol': 'NIFTY24FEB2724000PE', 'quantity': -50, 'average_price': 120.0},  # Weekly Feb 27
        {'symbol': 'NIFTY24MAR2420000CE', 'quantity': -25, 'average_price': 200.0},  # Monthly March
        {'symbol': 'BANKNIFTY24FEB2045000PE', 'quantity': -25, 'average_price': 300.0},  # BANKNIFTY weekly
    ]
    
    calculator = PortfolioGreeksCalculator(risk_free_rate=0.10)
    
    result = calculator.calculate_portfolio_greeks(
        positions=positions,
        underlying_price=24150.0
    )
    
    print(f"\nTotal positions: {result['total_positions']}")
    print(f"Calculated positions: {result['calculated_positions']}")
    print(f"Skipped positions: {len(result['skipped_positions'])}")
    
    print("\nPosition details:")
    for p in result['positions']:
        print(f"  {p['symbol']}: {p['index_name']} {p['expiry_type']} "
              f"expiry={p['expiry_date']} strike={p['strike']}")
    
    print("\nPortfolio Greeks:")
    totals = result['portfolio_totals']
    print(f"  Delta: {totals['delta']}")
    print(f"  Gamma: {totals['gamma']}")
    print(f"  Theta: {totals['theta']}")
    print(f"  Vega: {totals['vega']}")


if __name__ == '__main__':
    print("\n")
    
    # Run tests
    success = test_symbol_parsing()
    test_expiry_dates()
    test_portfolio_with_multiple_expiries()
    
    print("\n" + "=" * 80)
    if success:
        print("All tests passed!")
    else:
        print("Some tests failed!")
        sys.exit(1)
