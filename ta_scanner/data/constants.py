from enum import Enum
from trading_calendars import get_calendar, TradingCalendar
from typing import Dict


class TimezoneNames(Enum):
    US_EASTERN = "US/Eastern"
    US_CENTRAL = "US/Central"
    US_MOUNTAIN = "US/Mountain"
    US_PACIFIC = "US/Pacific"
    UTC = "UTC"


class WhatToShow(Enum):
    TRADES = "TRADES"
    MIDPOINT = "MIDPOINT"
    BID = "BID"
    ASK = "ASK"
    BID_ASK = "BID_ASK"
    ADJUSTED_LAST = "ADJUSTED_LAST"
    HISTORICAL_VOLATILITY = "HISTORICAL_VOLATILITY"
    OPTION_IMPLIED_VOLATILITY = "OPTION_IMPLIED_VOLATILITY"
    REBATE_RATE = "REBATE_RATE"
    FEE_RATE = "FEE_RATE"
    YIELD_BID = "YIELD_BID"
    YIELD_ASK = "YIELD_ASK"
    YIELD_BID_ASK = "YIELD_BID_ASK"
    YIELD_LAST = "YIELD_LAST"


class Exchange(Enum):
    SMART = "SMART"
    NYSE = "NYSE"
    GLOBEX = "GLOBEX"
    NYMEX = "NYMEX"
    ECBOT = "ECBOT"
    CBOE = "CBOE"
    ICE = "ICE"


class Calendar(Enum):
    # https://github.com/quantopian/trading_calendars
    DEFAULT = "XNYS"  # default to NYSE
    NYSE = "XNYS"
    CME = "CMES"
    CBOE = "XCBF"
    ICE = "IEPA"

    @staticmethod
    def futures_lookup_hash() -> Dict:
        return {
            Calendar.CME: [
                # equities
                "/ES",
                "/MES",
                "/MNQ",
                "/NQ",
                "/MNQ",
                # metals
                "/GC",
                "/MGC",
                # energy
                "/CL",
                "/QM",
                # currencies
                "/M6A",
                "/M6B",
                "/M6E",
                # interest rates
                "/GE",
                "/ZN",
                "/ZN",
                "/ZT",
                # grains
                "/ZC",
                "/YC",
                "/ZS",
                "/YK",
                "/ZW",
                "/YW",
            ],
            Calendar.CBOE: [],
            Calendar.ICE: [],
        }

    @staticmethod
    def select_exchange_by_symbol(symbol: str):
        for k, v in Calendar.futures_lookup_hash().items():
            if symbol in v:
                return k
        logger.warning(f"Did not find a calendar entry for symbol={symbol}")
        return Calendar.DEFAULT

    @staticmethod
    def init_by_symbol(symbol: str) -> TradingCalendar:
        if "/" in symbol:
            key = Calendar.select_exchange_by_symbol(symbol)
            name = key.value
        else:
            name = Calendar.NYSE.value
        return get_calendar(name)


class Currency(Enum):
    USD = "USD"
