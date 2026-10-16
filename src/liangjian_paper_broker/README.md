# Liangjian Local Paper Broker

Pure matching core for research, with no live broker SDK or project dependency.
Licensed under MIT; it is not an exchange-arrival or execution guarantee.

Inputs are an immutable order, a **closed** one-minute bar (volume in shares),
point-in-time trade-date rules, unreserved cash and the remaining **shared**
minute participation budget. Limit touches use next-bar VWAP bounded by the
limit and the plan's no-chase cap. T+1 sellable quantities are authoritative
ledger inputs. Commission minimum is cumulative per order, not per partial fill.

`ledger.LocalPaperBroker` commits cash, reservations, FIFO sellable lots, fills,
per-symbol bar identities and execution events in short SQLite transactions.
Repeated immutable inputs are no-ops; conflicting inputs and out-of-order bars
are rejected. Arrival time is explicit, preventing future-bar matching. EOD
settlement requires an actual 15:00 closing mark; missing marks produce null
NAV / DATA_LIMITED rather than an invented current price. Replay snapshots are
canonical JSON bytes, not byte equality of the physical SQLite file.

The project adapter reads the two authorized YAML files, checks the trading
calendar and arrival clock, and disables unimplemented brokers. Production
signal/risk wiring, six-day frozen replay, fault injection and release gates
remain pending. Do not call these local tests production execution acceptance.
