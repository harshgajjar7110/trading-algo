from brokers.core.enums import Exchange, OrderType, ProductType, TransactionType, Validity

# Dhan Constants
# Exchange Segment
NSE_EQ = "NSE_EQ"
NSE_FNO = "NSE_FNO"
NSE_CURRENCY = "NSE_CURRENCY"
BSE_EQ = "BSE_EQ"
BSE_FNO = "BSE_FNO"
BSE_CURRENCY = "BSE_CURRENCY"
MCX_COMM = "MCX_COMM"

# Order Type
MARKET = "MARKET"
LIMIT = "LIMIT"
STOP_LOSS = "STOP_LOSS"
STOP_LOSS_MARKET = "STOP_LOSS_MARKET"

# Product Type
CNC = "CNC"
INTRADAY = "INTRADAY"
MARGIN = "MARGIN"
CO = "CO"
BO = "BO"
MTF = "MTF"

# Transaction Type
BUY = "BUY"
SELL = "SELL"

# Validity
DAY = "DAY"
IOC = "IOC"

# Mappings
exchange = {
    Exchange.NSE: NSE_EQ,  # Default to EQ, logic in driver might need to adjust based on symbol
    Exchange.NFO: NSE_FNO,
    Exchange.BSE: BSE_EQ,
    Exchange.BFO: BSE_FNO,
    Exchange.MCX: MCX_COMM,
    Exchange.CDS: NSE_CURRENCY,
}

order_type = {
    OrderType.MARKET: MARKET,
    OrderType.LIMIT: LIMIT,
    OrderType.STOP_LIMIT: STOP_LOSS,
    OrderType.STOP: STOP_LOSS_MARKET,
}

product_type = {
    ProductType.CNC: CNC,
    ProductType.INTRADAY: INTRADAY,
    ProductType.MARGIN: MARGIN,
}

transaction_type = {
    TransactionType.BUY: BUY,
    TransactionType.SELL: SELL,
}

validity = {
    Validity.DAY: DAY,
    Validity.IOC: IOC,
}
