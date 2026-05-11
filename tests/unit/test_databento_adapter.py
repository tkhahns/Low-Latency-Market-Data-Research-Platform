from __future__ import annotations

from dataclasses import dataclass

from market_platform.databento_adapter import raw_events_from_databento


@dataclass
class Level:
    pretty_bid_px: float
    pretty_ask_px: float
    bid_sz: int
    ask_sz: int


class MBP1Msg:
    instrument_id = 101
    ts_event = 1_700_000_000_123_456_789
    sequence = 42
    pretty_price = 4550.25
    size = 3
    action = "T"
    side = "B"
    levels = [Level(pretty_bid_px=4550.0, pretty_ask_px=4550.25, bid_sz=12, ask_sz=8)]


class TradeMsg:
    instrument_id = 101
    ts_event = 1_700_000_000_123_456_789
    sequence = 43
    pretty_price = 4550.5
    size = 2
    action = "T"
    side = "A"


class SymbolMappingMsg:
    instrument_id = 101
    stype_in_symbol = "ES.FUT"
    stype_out_symbol = "123456"


def test_symbol_mapping_record_updates_map_without_publishing_events() -> None:
    symbol_map: dict[int, str] = {}

    assert raw_events_from_databento(SymbolMappingMsg(), symbol_map, "GLBX") == []

    assert symbol_map == {101: "ES.FUT"}


def test_mbp1_trade_action_emits_quote_and_trade() -> None:
    symbol_map = {101: "ES.FUT"}

    events = raw_events_from_databento(MBP1Msg(), symbol_map, "GLBX")

    assert [event["event_type"] for event in events] == ["quote", "trade"]
    assert events[0]["symbol"] == "ES.FUT"
    assert events[0]["bid_price"] == 4550.0
    assert events[0]["ask_size"] == 8
    assert events[1]["price"] == 4550.25
    assert events[1]["sequence_number"] == 42


def test_trade_record_emits_trade() -> None:
    symbol_map = {101: "ES.FUT"}

    events = raw_events_from_databento(TradeMsg(), symbol_map, "GLBX")

    assert len(events) == 1
    assert events[0]["event_type"] == "trade"
    assert events[0]["symbol"] == "ES.FUT"
    assert events[0]["price"] == 4550.5
