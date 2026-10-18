# Beginner Quant Research Assignment: Cross-Market Arbitrage Analysis

## Objective

Build a basic data pipeline for traditional equities and crypto/perpetual markets, then investigate whether actionable arbitrage opportunities exist between equivalent instruments listed on different venues.

## Tasks

### 1. Set Up IBKR Market Data Access

- Open an Interactive Brokers (IBKR) account. Use a paper-trading account whenever possible.
- Install and configure the IBKR API using either Trader Workstation or IB Gateway(recommand).
- Write a Python program that connects to the API and retrieves:
  - Bid and ask prices
  - Last traded price
  - Timestamps
  - Trading volume, if available
- Save the collected data in CSV, Parquet, or a database.
- Document the account configuration, API setup, market-data subscriptions, and any data limitations.

### 2. Set Up Hyperliquid Market Data Access

- Create a new cryptocurrency wallet（metamask/phantom）. Never share or commit the seed phrase or private key.
- Connect the wallet to Hyperliquid and create a trading account.
- Use the Hyperliquid API to retrieve:
  - Bid and ask prices
  - Last traded price
  - Timestamps
  - Trading volume
  - Funding rates and open interest, when available
- Start with public market-data endpoints. Use testnet or very small amounts if wallet signing or trading functionality must be tested.
- Save the data using the same structure and timestamp standard as the IBKR data.

### 3. Investigate Potential Arbitrage Opportunities

Analyze the following instrument pairs:

- SNDK vs. SNKD.US
- NVDA vs. NVDA.US
- TSLA vs. TSLA.US
- NBIS vs. NBIS.US
- META vs. META.US

Before analyzing them, verify that every ticker is correct and determine exactly what each instrument represents. In particular, confirm whether `SNDK vs. SNKD.US` contains a ticker typo.

For every pair:

1. Confirm that both instruments track the same underlying economic exposure.
2. Identify the venue, currency, contract type, multiplier, trading hours, settlement method, and funding mechanism.
3. Normalize prices into the same currency and unit.
4. Synchronize observations using timestamps. Do not compare stale or unmatched prices.
5. Calculate both executable arbitrage directions:

   $$
   \text{Spread A to B} = \text{Bid}_B - \text{Ask}_A
   $$

   $$
   \text{Spread B to A} = \text{Bid}_A - \text{Ask}_B
   $$

6. Report the absolute and percentage spreads.
7. Estimate the net spread after:
   - Bid-ask spreads
   - Trading commissions and exchange fees
   - Currency-conversion costs
   - Funding payments
   - Borrowing or short-selling costs
   - Slippage
   - Transfer or settlement costs
8. Identify when positive net spreads occurred, how large they were, and how long they lasted.
9. Explain whether the opportunity was realistically executable, considering liquidity, market hours, latency, position limits, and execution risk.

While the codebase can be generated with AI, the research stage should rely on visual analytics dashboards/charts to identify and validate cross-market arbitrage opportunities using executable bid/ask data. Do not classify a difference between last-traded prices as an arbitrage opportunity unless it is also supported by executable bid and ask prices.

### 4. Deliverables

Submit:

- All Python source code
- A `README` containing setup and execution instructions
- Sample raw and cleaned datasets
- A table of detected opportunities
- Charts showing spreads over time
- A short written summary of findings
- A presentation covering:
  1. Research objective
  2. System architecture and data sources
  3. Instrument and ticker validation
  4. Data-cleaning and synchronization methodology
  5. Spread calculation
  6. Fees and other execution costs
  7. Results for each pair
  8. Risks, limitations, and possible false signals
  9. Conclusions and recommended next steps

Be prepared to present the results and demonstrate the data-collection pipeline.

## Success Criteria

The assignment is complete when:

- Both APIs can be queried reliably.
- Timestamps, currencies, and contract specifications are normalized correctly.
- Every ticker and instrument is independently verified.
- Opportunities are evaluated using executable bid/ask prices and net transaction costs.
- The analysis is reproducible from the submitted code and instructions.
- The final presentation clearly distinguishes theoretical price differences from realistically executable arbitrage opportunities.
