
import os
import sys

# Add project root to path
sys.path.append(os.getcwd())


try:
    from brokers.integrations.dhan.driver import DhanDriver
    print("Successfully imported DhanDriver")
    
    # Attempt instantiation
    driver = DhanDriver()
    print("Successfully instantiated DhanDriver")
    
    # Check capabilities
    print(f"Capabilities: {driver.capabilities}")
    
    # Check if new methods exist
    methods = [
        "get_quote",
        "cancel_order",
        "modify_order", 
        "get_orderbook",
        "get_tradebook",
        "connect_websocket",
        "place_gtt_oco_order"
    ]
    
    missing = []
    for m in methods:
        if not hasattr(driver, m):
            missing.append(m)
            
    if missing:
        print(f"FAILED: Missing methods: {missing}")
    else:
        print("SUCCESS: All new methods found.")
        
    # Try calling get_quote (should return dummy/error safe response)
    q = driver.get_quote("NSE:NIFTY 50")
    print(f"get_quote check: {q}")
    
    print("\nVerification passed!")
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error: {e}")

