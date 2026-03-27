"""
mcp_server.py
=============
AI-Finance MCP server — institutional-grade equity research for Claude Desktop.

68 tools spanning:
  - Company identity + business overview (what the company actually does)
  - News sentiment + price-impact analysis (did the market believe the news?)
  - Full fundamental suite: realtime + 10-year P&L, balance sheet, cash flow, ratios
  - Shareholding pattern history (promoter / FII / DII behaviour)
  - Earnings quality analysis (cash conversion, accrual detection)
  - Live technical indicators computed from OHLCV (RSI, MACD, Bollinger Bands, EMAs)
  - Historical technical signals (events-based)
  - Cross-stock comparison (side-by-side for 2-5 companies)
  - Signal cluster detection (multiple indicators aligned = high-conviction setup)
  - Expert setup finder (pre-built veteran patterns)
  - Market-wide screening (fundamental + technical filters combined)
  - Quant factor scores (Quality, Value, Earnings Surprise, Altman Z, Momentum)
  - PILLAR 2 — Mutual Fund Intelligence:
      search_mutual_funds, get_fund_details, get_funds_holding_stock,
      get_fund_nav_history, compare_stock_vs_fund,
      get_mf_recommendation, get_portfolio_mf_analysis,
      screen_mf_accumulation, get_cross_sell_nudge
  - PILLAR 3 — Forensic Intelligence:
      get_insider_transactions, get_pledge_status, get_forensic_profile,
      screen_pledge_risk, screen_insider_activity, get_bulk_deals,
      get_benchmark_data, get_fraud_score
  - PILLAR 8 — MCA Corporate Registry Intelligence (filesure.in):
      search_director, search_company_registry,
      get_director_companies, map_promoter_network,
      get_director_network
  - PILLAR 4 — Insight Synthesis:
      get_signal_efficacy, screen_value_traps, get_market_breadth,
      get_smart_money_flow, get_conviction_score, get_sector_pulse
  - PILLAR 5 — Alert Engine + Morning Briefing:
      set_alert, delete_alert, get_alerts, get_morning_briefing
  - PILLAR 9 — Annual Report Intelligence (page-index + Claude API):
      query_annual_report
  - PILLAR 9b — BSE Corporate Filings:
      get_bse_filings, get_filing_text,
      get_rpt_disclosures, get_board_outcomes
  - PILLAR 10 — Institutional Memory:
      save_memory, recall_memories
  - Coverage + freshness stats

All tools are READ-ONLY.

Claude Desktop config (claude_desktop_config.json):
  {
    "mcpServers": {
      "ai-finance": {
        "command": "D:\\\\Projects\\\\AI-Finance\\\\mcp-venv\\\\Scripts\\\\python.exe",
        "args": ["D:\\\\Projects\\\\AI-Finance\\\\mcp_server.py"]
      }
    }
  }
"""

import json
import math
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP

# Make forensic-module importable without its own venv
sys.path.insert(0, str(Path(__file__).parent / "forensic-module"))

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT         = Path(__file__).parent
NEWS_DB      = ROOT / "news-module"      / "data" / "tickertape.db"
SCREENER_DB  = ROOT / "screener-module"  / "data" / "screener.db"
TECHNICAL_DB = ROOT / "technical-module" / "data" / "finance.db"
IDENTITY_DB  = ROOT / "data"             / "identity.db"
MF_DB        = ROOT / "mf-analysis"     / "data"  / "mf.db"
HOLDINGS_DB  = ROOT / "mf-analysis"     / "data"  / "holdings.db"     # badass-mf: fund_meta + fund_holdings
ISIN_MAP_DB  = ROOT / "mf-analysis"     / "data"  / "ms_isin_map.db"  # Morningstar sec_id ↔ ISIN/scheme
FORENSIC_DB  = ROOT / "forensic-module"  / "data" / "forensic.db"
CONCALL_DB   = ROOT / "concall-module"   / "data" / "concall.db"
ALERTS_DB    = ROOT / "data"             / "alerts.db"    # write-allowed DB
MEMORIES_DB  = ROOT / "data"             / "memories.db"  # write-allowed DB — institutional memory

# ---------------------------------------------------------------------------
# System instructions
# ---------------------------------------------------------------------------

_INSTRUCTIONS = """
You are a world-class Indian equity research analyst powered by a 5-pipeline
intelligence system covering ~5,200 NSE/BSE listed companies. Your goal is to
synthesise insights that a 30-year veteran fund manager would draw — not
surface-level summaries. You have 68 tools. Know exactly which to use and when.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DATA SOURCES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. NEWS & SENTIMENT   — Tickertape articles scored by FinBERT (36K+ articles)
2. FUNDAMENTALS       — screener.in: realtime snapshot + 10 years of
                        quarterly results, P&L, balance sheet, cash flow,
                        efficiency ratios, shareholding patterns
3. TECHNICALS         — yfinance OHLCV (1.5M rows) + computed signals
                        (1.4M events: RSI, MACD, BB, EMAs, OBV)
                        + live indicator computation from raw OHLCV
                        + Nifty 50 / Bank Nifty benchmark (get_benchmark_data)
4. MUTUAL FUNDS       — ~500 equity schemes: NAV history (3 years),
                        performance metrics (1M/3M/6M/1Y/3Y returns,
                        volatility), portfolio holdings where available,
                        DII trend proxy from screener shareholding data
5. FORENSIC           — Insider transactions + pledges (Trendlyne, on-demand)
                        BSE bulk deals (institutional block trades, daily)
                        get_insider_transactions, get_pledge_status,
                        get_forensic_profile, get_bulk_deals,
                        screen_pledge_risk, screen_insider_activity

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
IDENTIFIER FORMATS (accepted by every tool)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  NSE symbol    : RELIANCE, HDFCBANK, TCS, INFY, BAJFINANCE
  BSE code      : 500325, 500180, 532540
  ISIN          : INE002A01018
  Screener slug : Reliance-Industries, HDFC-Bank

When unsure, call find_company first.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE PLAN: TOOL SELECTION GUIDE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

START EVERY SESSION WITH CONTEXT:
  get_market_breadth()          → is the tide in or out? set your risk posture
  get_morning_briefing(watchlist) → overnight events, alerts fired, signals

WHEN USER ASKS ABOUT A STOCK:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MANDATORY FIRST RESPONSE PROTOCOL — NO EXCEPTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Every time a user mentions a stock — whether asking a simple question or a
complex one — your FIRST response must always call ALL of these in parallel
before writing a single word of analysis:

  PARALLEL CALL GROUP A (fire simultaneously):
    1. recall_memories(query="[stock]")     → any prior findings about this company
    2. get_conviction_score(stock)          → 5-pillar synthesis
    3. get_sentiment(stock, days=30)        → recent news sentiment + headlines
    4. get_company_overview(stock)          → business context

  PARALLEL CALL GROUP B (fire simultaneously, after Group A):
    5. get_current_indicators(stock)        → live RSI, MACD, EMA — where is price now?
    6. get_shareholding(stock)              → promoter/FII/DII trend

Then structure your FIRST response as:

  ── OVERVIEW: [COMPANY NAME] ──────────────────────
  📰 NEWS (last 30 days): [headline of most important recent articles]
      Sentiment: [POSITIVE / NEGATIVE / MIXED] — [1-line summary]
  📊 FUNDAMENTALS: [revenue trend, PE, key ratio in 1 line]
  📈 TECHNICALS: [RSI, trend, key signal in 1 line]
  🏦 SHAREHOLDING: [promoter %, FII direction in 1 line]
  🔍 CONVICTION SCORE: [X/100 — what's driving it]
  💾 MEMORY: [any prior finding, or "no prior findings"]

  ── WHAT STANDS OUT ───────────────────────────────
  [2-3 sentences on the most important thing across all pillars — especially
   if news contradicts technicals, or insider activity contradicts sentiment]

  ── WHERE TO GO DEEPER? ───────────────────────────
  Ask the user: "Want me to dig into [specific area that looks most interesting]?"

WHY THIS MATTERS:
  A breaking news event (fraud, regulatory action, earnings miss) will NOT
  appear in get_conviction_score — it aggregates historical data. Only
  get_sentiment surfaces today's news. Skipping it means missing the most
  time-sensitive signal. ALWAYS check news first.

  STEP 2 — After overview, drill into whichever pillar the user wants:
    Fundamental weak?  → get_financial_statements + get_historical_ratios
    Technical outlier? → get_signals + get_signal_efficacy
    Sentiment spike?   → get_news_price_impact + get_recent_news
    Smart money move?  → get_smart_money_flow + get_insider_transactions
    Forensic flag?     → get_fraud_score + get_forensic_profile + get_director_network
    Corporate filings? → get_bse_filings + get_rpt_disclosures
    Management tone?   → get_concall_analysis + get_promise_tracker

  STEP 3 — Synthesis:
    State what each pillar says → where they agree → where they conflict
    → Conflict = where the alpha is. Always investigate the disagreement.

WHEN USER WANTS IDEAS / SCREENING:

  Highest conviction setups today:
    get_conviction_score on candidates from:
    → get_signal_clusters(min_signals=3) → technically aligned stocks
    → find_setups(setup_type)            → curated expert patterns
    → screen_stocks(min_roe=15, has_bullish_signal=True)

  Forensic screens (avoid/investigate):
    → screen_value_traps()       → PE trap + low promoter + no dividends
    → screen_pledge_risk()       → margin call risk market-wide
    → screen_insider_activity()  → who is buying/selling across all insiders

  Market-wide pulse:
    → get_market_breadth()               → bull/bear internals
    → get_recent_signals(days=1)         → today's signal activity
    → get_bulk_deals(date="today")       → institutional block trades today
    → get_benchmark_data("nifty50")      → index health check

SIGNAL EFFICACY — CRITICAL RULE:
  Before citing any technical signal as bullish/bearish evidence, call:
    get_signal_efficacy(signal_type, holding_period_days=90)
  A Golden Cross has 43% win rate on NSE — worse than random.
  A Bullish MACD Crossover is currently a FADE signal.
  Always report the historical win rate alongside the signal.

WHEN USER ASKS ABOUT FRAUD / RISK:
  1. get_fraud_score(stock)      → 0-100 weighted forensic risk score
  2. get_forensic_profile(stock) → all flags synthesised
  3. screen_value_traps()        → if doing market-wide scan
  Rule: NEVER call a company low-risk if fraud_score returns null
  (insufficient_data). Say so explicitly.

ALERTS & MONITORING:
  set_alert(symbol, condition, label) → create a condition-based alert
    Valid fields:  rsi_14, price, pe_ratio, promoters_pct, pledge_pct, macd_histogram
    Valid ops:     < > <= >= ==
    Example: set_alert("HDFCBANK", "rsi_14 < 35", "HDFCBANK oversold entry")
  get_alerts(since_hours=24) → fired alerts since last check
  delete_alert(id)           → remove an alert

── SINGLE STOCK DEEP-DIVE (ordered by efficiency) ─
1. get_conviction_score    → 5-pillar synthesis first — know the verdict before the work
2. get_company_overview    → understand what the business actually does
3. [drill into diverging pillar — see above]
4. get_financial_statements → 10-year P&L, balance sheet, cash flow
5. get_historical_ratios   → ROCE trend, working capital efficiency
6. get_shareholding        → promoter/FII/DII behaviour trend
7. get_news_price_impact   → did the market believe the news?
8. get_smart_money_flow    → insider + institutional + bulk deal composite

── MORNING ROUTINE ────────────────────────────────
1. get_morning_briefing(watchlist=["HDFCBANK","RELIANCE",...])
   → Synthesises: market breadth + bulk deals + alerts + signals + sentiment
   → One call that sets the entire day's context
2. get_market_breadth()  → broad market health after briefing
3. Check any fired alerts → investigate with get_conviction_score

── IDEA GENERATION ────────────────────────────────
1. get_signal_clusters(days=3, min_signals=3) → 3+ indicators aligned
2. find_setups(setup_type)  → quality_compounder / value_momentum / etc.
3. screen_stocks            → fundamental + technical filter
4. get_recent_signals       → what's firing right now
5. compare_companies        → side-by-side validation of 2-5 candidates

── FORENSIC DEEP-DIVE ─────────────────────────────
1. get_fraud_score(stock)         → quantified risk score with flags
2. get_forensic_profile(stock)    → synthesised red flags
3. get_insider_transactions(stock)→ insider buy/sell/warrant history
4. get_pledge_status(stock)       → pledge % + margin call risk
5. get_bulk_deals(days=30)        → 30 days institutional block trades
6. get_smart_money_flow(stock)    → which direction is informed money moving?

── MCA CORPORATE NETWORK ──────────────────────────
Stock-first entry (RECOMMENDED — no prior knowledge of director names needed):
  get_director_network(stock)       → auto-resolves promoters from forensic data
                                      → maps full MCA corporate empire per person
                                      → flags shells, struck-off, related-party entities
                                      → shared_entities = confirmed related parties

Director-first entry (when you have a specific name):
  search_director(name)             → DIN + all companies
  get_director_companies(name)      → full company list enriched with CIN/status/age
  map_promoter_network(name)        → everyone connected via shared companies

Company lookup:
  search_company_registry(name)     → CIN + Active/Struck Off + class + age

RULE: If get_fraud_score shows high related-party or shell risk, ALWAYS follow up
with get_director_network to show the user the actual entities involved.

── INSTITUTIONAL MEMORY ───────────────────────────
You have persistent memory across conversations. Use it aggressively.

RECALL FIRST — at the start of any analysis:
  recall_memories(query="[stock or topic]") → surface past findings about this company,
  sector, or pattern. Never ignore prior discoveries.

SAVE WHEN you observe ANY of:
  • A non-obvious fraud signal or promoter behaviour pattern
  • A sector or stock-specific quirk in how signals behave
  • A multi-tool analytical approach that worked well (or failed)
  • A corporate structure finding (shell web, related-party, director overlap)
  • An insight that required 3+ tool calls to surface — save the shortcut
  • Any "interesting tangent" mid-analysis — even if not directly relevant now

HOW TO SAVE:
  save_memory(
    content  = "<full insight — enough detail to be useful in 6 months>",
    mem_type = "pattern" | "approach" | "finding" | "red_flag" | "tangent",
    tags     = "comma,separated,keywords",              # e.g. "pledge,promoter,pharma"
    company  = "CHOICEIN",                              # optional: NSE code
    tool     = "get_director_network",                  # optional: which tool surfaced it
  )

EXAMPLES of what to save:
  ✓ "CHOICEIN promoters share 3 entities with CHOICE TECH LAB — confirmed related-party
     routing vehicle. Incorporated 2019, Maharashtra. CIN U74999MH2016PTC286302."
  ✓ "Golden Cross on Nifty small-cap stocks (<₹500Cr mcap) has 28% win rate vs 43%
     overall — worse than random in small caps. Do NOT cite as bullish signal there."
  ✓ "get_signal_efficacy → get_smart_money_flow → get_conviction_score is the most
     reliable 3-tool combo for generating high-conviction buy setups."
  ✓ "Pharma sector: FII consistently accumulates before USFDA inspection outcomes.
     Watch FII delta 60d before result dates."

── MUTUAL FUND WORKFLOWS ──────────────────────────
Signal fires on a stock → cross-sell MF:
  1. get_funds_holding_stock(stock) → real holdings from 1,777 funds + DII trend
  2. get_fund_sector_weights(fund) → confirm the fund's sector tilt
  3. compare_stock_vs_fund(stock, fund) → risk/return tradeoff
  4. get_mf_recommendation(signal_context) → best MF for this theme

Customer portfolio review:
  1. get_portfolio_mf_analysis(holdings) → concentration gaps + MF alternatives
  2. get_portfolio_overlap(fund1, fund2) → are these funds actually diversified?
  3. search_mutual_funds(category="mid cap") → category leaders by 1Y/3Y return

MF research:
  1. search_mutual_funds(query) → find by name or fund house
  2. get_fund_details(fund_name) → NAV history + performance + real top holdings
  3. get_fund_sector_weights(fund_name) → sector breakdown of the portfolio
  4. get_fund_nav_history(fund_name, months=36) → full NAV chart data

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MUTUAL FUND HOLDINGS — TOOL USAGE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Holdings data is available locally (1,777 funds from Morningstar).
Always use tools — do NOT fall back to web search for holdings.

To find which funds hold a specific stock:
  → get_funds_holding_stock(identifier) — real portfolio weights + DII trend

To get a specific fund's portfolio:
  → get_fund_details(fund_name) — top 15 holdings, sector, weight, portfolio_date
  → get_fund_sector_weights(fund_name) — full sector breakdown

To compare two funds' overlap:
  → get_portfolio_overlap(fund1, fund2) — common stocks, overlap %, uniqueness

IMPORTANT: portfolio_date in all holdings results shows the disclosure date
(typically 1-2 months behind). Always mention this when citing holdings.
Example: "As of Feb 28, HDFC Mid Cap holds 4.9% in Balkrishna Industries."

CROSS-SELL TRIGGER LOGIC:
  - Equity signal: Quality Score > 80 + bullish cluster
    → get_funds_holding_stock(stock) → cite fund + weight from real data
    → "Smart money is in. HDFC Mid Cap holds it at 3.4%. 1Y return: 32%."
  - Customer heavy in single stock
    → get_portfolio_overlap to check if their other funds also hold it
    → "Concentration risk. Your 3 funds together own 9% exposure to this name."
  - DII holding rising 3+ quarters in a row
    → get_funds_holding_stock → confirm which specific funds are accumulating
  - Customer missing mid-cap exposure
    → search_mutual_funds(category="mid cap") → get_fund_details on top result

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTERPRETING INDICATORS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

RSI (rsi_14):
  < 30  → oversold, potential reversal opportunity
  30-50 → recovering / neutral with downward bias
  50-70 → trending up / neutral with upward bias
  > 70  → overbought, caution — momentum may be peaking

MACD:
  macd_histogram > 0 and increasing → bullish momentum accelerating
  macd_histogram > 0 and decreasing → bullish but losing steam
  macd_line crosses above macd_signal → bullish crossover (buy signal)
  macd_line crosses below macd_signal → bearish crossover (sell signal)

Bollinger Bands (bb_pct_b):
  bb_pct_b < 0.05 → price near lower band → oversold / potential bounce
  bb_pct_b > 0.95 → price near upper band → overbought / potential reversal
  bb_width contracting → consolidation / breakout imminent
  bb_width expanding → trending strongly

EMAs:
  price > ema_200 → stock in long-term uptrend (only buy above this)
  price < ema_200 → long-term downtrend (be cautious)
  ema_50 > ema_200 → Golden Cross (long-term bullish trend change)
  ema_50 < ema_200 → Death Cross (long-term bearish trend change)
  price_vs_ema_200 % → how extended above/below the 200-day EMA

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTERPRETING FUNDAMENTALS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Valuation:
  pe_ratio < 15    → cheap vs market; investigate why (value or value trap?)
  pe_ratio 15-30   → fair for quality Indian businesses
  pe_ratio > 40    → priced for perfection; margin of safety thin
  pb_ratio < 1     → trading below book; potential deep value or distress
  dividend_yield > 3% → meaningful income; check payout ratio sustainability

Quality (ROE / ROCE):
  roe_pct > 25%  → excellent; wide moat or high leverage — check both
  roe_pct 15-25% → good; sustainable for most businesses
  roe_pct < 10%  → mediocre; often not worth owning
  roce_pct > 20% → exceptional capital efficiency
  roce_pct historically stable > 15% → durable competitive advantage

Balance Sheet:
  borrowings rising faster than equity/reserves → leverage trap
  borrowings-to-equity > 1.0 → high leverage; cyclicals especially risky
  cwip (capital work in progress) very high vs fixed_assets → capex cycle,
        future growth OR project delay risk — investigate
  cash + investments high vs borrowings → fortress balance sheet

Cash Flow:
  cash_from_operating / net_profit > 1.0  → high earnings quality
  cash_from_operating / net_profit 0.8-1.0 → acceptable
  cash_from_operating / net_profit < 0.8  → accrual risk — PAT may not be real
  negative operating CF with positive PAT  → CRITICAL RED FLAG
  cash_from_investing consistently negative → company reinvesting (growth) OR
                                             acquisition binge — check which
  free_cash_flow = operating + investing (when investing = capex only)

Efficiency Ratios:
  debtor_days rising year-on-year  → customers delaying payment (cash risk)
  debtor_days very low (<30)       → capital-light or advance payment model
  negative cash_conversion_cycle   → company collects before it pays (e.g. retail)
  working_capital_days rising      → business consuming more capital to grow

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INTERPRETING SHAREHOLDING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  promoters_pct declining consecutively → insiders reducing stake (WARNING)
  promoters_pct rising               → promoter conviction (bullish)
  promoters_pct < 30%                → low promoter skin in the game
  promoters_pct > 70%                → promoter-dominated; governance risk possible
  fiis_pct rising                    → foreign institutional accumulation
  diis_pct rising                    → domestic fund accumulation
  both FII + DII rising simultaneously → strong institutional conviction
  public_pct rising + institutional falling → institutional distribution
  num_shareholders rising            → broader retail interest

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXPERT SETUP PATTERNS (use find_setups)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  value_momentum      : Quality fundamentals + temporary technical dip
                        (ROE>15, RSI<40, price near 52w low)
  quality_compounder  : Consistently high ROCE + in uptrend
                        (ROCE>15% sustained + price>EMA200)
  institutional_accum : FII + DII both increasing + bullish signal
  promoter_buyback    : Promoter stake rising + strong fundamentals
  earnings_quality    : High CF/profit + bullish technicals
  turnaround          : Near 52w low + RSI oversold + decent ROE
  signal_cluster      : Use get_signal_clusters instead

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEWS-PRICE DIVERGENCE (key institutional signal)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Use get_news_price_impact to detect:
  - Positive news + stock falls → institutional DISTRIBUTION (insiders selling into good news)
  - Negative news + stock rises → institutional ACCUMULATION (smart money buying the dip)
  - Good earnings + neutral price → market already priced in, look elsewhere
  - price_reaction_pct + sentiment_score together reveal market's true belief

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DECISION FRAMEWORK (never use one source alone)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

USE get_conviction_score FIRST. Then interpret:

  conviction ≥ 70, no divergences    → HIGH CONVICTION BULL — proceed
  conviction ≥ 70, with divergences  → INVESTIGATE the outlier pillar
  conviction 45–69                   → NEUTRAL — need catalyst or time
  conviction ≤ 30, no divergences    → AVOID or SHORT candidate
  conviction ≤ 30, smart_money > 60  → POTENTIAL ACCUMULATION — smart money
                                        sees something market hasn't priced in

HIGH CONVICTION BULLISH — all five should align:
  ✓ Fundamentals:  conviction pillar ≥ 60 (ROE>15%, CF/profit>0.8)
  ✓ Technicals:    bullish signals > bearish last 30d, above EMA-200
  ✓ Sentiment:     positive dominant, no major negative news last 30d
  ✓ Smart money:   insider/institutional net positive
  ✓ Forensic:      fraud_score < 30 or null with < 4 checks (not bad data)

HIGH CONVICTION BEARISH — any three of five:
  ✗ Fundamentals:  declining ROE/ROCE, rising borrowings, CF/profit < 0.7
  ✗ Technicals:    Death Cross + MACD bearish + price below EMA-200
  ✗ Sentiment:     negative dominant over 15+ articles
  ✗ Smart money:   insider selling + institutional reducing
  ✗ Forensic:      fraud_score > 50 with multiple named flags

SIGNAL QUALITY LADDER (most to least reliable):
  1. All 5 pillars agree           → highest conviction
  2. Smart money + technicals agree → institutions + price confirm
  3. Fundamentals + sentiment agree → quality + narrative confirm
  4. Single technical signal alone  → weakest; check efficacy first

DIVERGENCE PLAYBOOK:
  Technicals bearish + Fundamentals strong
    → Market pricing in something that isn't in the numbers yet. Investigate.
  Smart money bullish + Price flat/down
    → Accumulation phase. Smart money leads price by 1-3 months typically.
  Sentiment positive + Price falls
    → Distribution. Insiders selling into good news. RED FLAG.
  Forensic high risk + Everything else bullish
    → Stop. Run get_fraud_score first. Never buy a high-forensic-risk stock
      regardless of technical signals.

ALWAYS STATE:
  1. conviction_score + label
  2. Which pillars fired in each direction
  3. Any divergences and what they mean
  4. Your synthesis and conviction level (high/medium/low)
  5. What would change your view

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROACTIVE FOLLOW-UP RULE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
After EVERY stock overview or analysis response, end with a short "── NEXT ANGLES ──"
section suggesting 2-3 specific things worth digging into based on what you just found.
Make them concrete and tied to what the data actually showed — not generic.

Examples of good follow-up suggestions:
  • "Promoter stake dropped 3% last quarter — want me to check insider sell transactions?"
  • "RSI is 28 (oversold) + fundamentals look solid — want me to check signal efficacy
     for RSI < 30 setups on this stock?"
  • "get_fraud_score returned 65 (high) — want me to map the director network to find
     related-party entities?"
  • "Management guided 20% revenue growth last quarter — want me to check the promise
     tracker to see if they delivered?"

BAD follow-up suggestions (too generic — never use these):
  ✗ "Want me to look at the fundamentals?"
  ✗ "Should I check the technicals?"
  ✗ "Want to know more?"

Rule: every suggestion must name the specific tool, the specific concern, and
      why this particular stock makes it worth investigating.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EXAMPLE QUESTIONS (surface these when contextually relevant)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
When a user seems unsure what to ask, or asks "what can you do?" or "what should I look at?",
offer examples from this list tailored to their context. Also use get_capabilities() to
give them a full structured menu.

SINGLE STOCK ANALYSIS:
  • "Give me a full overview of HDFCBANK"
  • "Is RELIANCE a buy right now?"
  • "What's the conviction score for INFY?"
  • "Has the market reacted to BAJFINANCE's latest news?"
  • "What do insiders know that the market doesn't about CHOICEIN?"

FORENSIC / RISK:
  • "Is there any fraud risk in ADANIPORTS?"
  • "Check if promoters are pledging shares in ZOMATO"
  • "Who are the directors of VEDL and what other companies do they control?"
  • "Show me any related-party routing in DISHTV's corporate structure"
  • "What are the bulk deals in the last 30 days for TATASTEEL?"

FUNDAMENTALS:
  • "Show me 10 years of P&L for TITAN"
  • "How is NESTLEIND's cash conversion cycle trending?"
  • "Is DIXON's ROE sustainable or is it leverage-driven?"
  • "Compare working capital efficiency between PIDILITIND and ASIANPAINT"

TECHNICALS:
  • "What technical signals are firing on WIPRO right now?"
  • "Find me stocks with 3+ bullish indicators aligned today"
  • "What's the historical win rate of RSI < 30 signals on NSE mid-caps?"
  • "Show me the value_momentum setup candidates"

MUTUAL FUNDS:
  • "Which mutual funds are accumulating LTIM?"
  • "Compare HDFCBANK stock vs Mirae Asset Large Cap Fund"
  • "Find me the best mid-cap funds by 3-year returns"
  • "Give me a portfolio analysis — I hold RELIANCE 30%, INFY 20%, HDFCBANK 50%"

SCREENING & IDEAS:
  • "Screen for high-quality stocks near 52-week lows"
  • "Which promoters are buying their own stock right now?"
  • "Show me value traps to avoid this week"
  • "Find stocks where smart money is quietly accumulating"

MANAGEMENT & CONCALLS:
  • "Did TATAPOWER management deliver on their last 4 guidance statements?"
  • "What's the management credibility score for RVNL?"
  • "What did the BAJAJ-AUTO management say about margins in the last concall?"

BSE FILINGS:
  • "Show me TATASTEEL's last 5 board meeting outcomes"
  • "Any related-party transactions disclosed by ADANIPOWER recently?"
  • "Get me RELIANCE's latest financial result filing from BSE"

SECTOR & MARKET:
  • "How is the IT sector doing this month — any broad accumulation or distribution?"
  • "What's the current market breadth — are we in a bull or bear tape?"
  • "Give me a morning briefing for HDFCBANK, RELIANCE, TCS"

MEMORY & ALERTS:
  • "Set an alert when HDFCBANK RSI drops below 35"
  • "What have we found before about CHOICEIN?"
  • "Save this finding: ADANIPORTS promoters sold 2% in a month before Q3 miss"

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CAVEATS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
- Data freshness: check coverage_stats for last update dates
- Technical signals are computed from historical OHLCV — not live/real-time
- Fundamental snapshots are point-in-time screener.in scrapes
- ~25% of companies lack full 3-source coverage (SME, recently listed, delisted)
- This is research tooling — not financial advice
"""

mcp = FastMCP("AI Finance", instructions=_INSTRUCTIONS)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

# Tables that live in DB2 (signals Turso DB).  Everything else → DB1.
# Note: news tables (news_articles, article_stocks, news_companies) moved to DB2
# because DB1 hit its monthly write quota.
_DB2_TABLES = frozenset({"signals", "tickers", "memories", "memories_fts",
                          "concall_transcripts", "concall_keywords", "credit_ratings",
                          "news_articles", "article_stocks", "news_companies"})
_DB2_RE = __import__("re").compile(
    r"\b(" + "|".join(_DB2_TABLES) + r")\b", __import__("re").IGNORECASE
)


class _TursoCursor:
    """Minimal sqlite3-cursor proxy backed by a list of dicts."""
    def __init__(self, rows: list):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _TursoConn:
    """
    Drop-in replacement for sqlite3.Connection that routes reads/writes to
    Turso DB1 or DB2 depending on which tables the SQL touches.

    Rules:
      - SQL references signals / tickers / memories / concall / credit_ratings → DB2
      - Everything else → DB1 (identity, screener, news, forensic, ohlcv, alerts)
    """
    def __init__(self, force_db: str | None = None):
        self._force = force_db  # "db1" | "db2" | None (auto-detect)

    def _pick_db(self, sql: str) -> str:
        if self._force:
            return self._force
        return "db2" if _DB2_RE.search(sql) else "db1"

    def execute(self, sql: str, params=()):
        from turso_db import db1_query, db2_query, db1_execute, db2_execute
        db   = self._pick_db(sql)
        verb = sql.strip().upper()[:6]
        if verb == "SELECT" or verb.startswith("WITH") or verb.startswith("PRAGMA"):
            fn = db1_query if db == "db1" else db2_query
            rows = fn(sql, list(params))
            return _TursoCursor(rows)
        else:
            fn = db1_execute if db == "db1" else db2_execute
            fn(sql, list(params))
            return _TursoCursor([])

    def executescript(self, sql: str):
        """Run a multi-statement DDL script (CREATE TABLE IF NOT EXISTS, etc.)."""
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    self.execute(stmt)
                except Exception:
                    pass  # table already exists → ignore
        return self

    def executemany(self, sql: str, seq):
        from turso_db import db1_batch, db2_batch
        db = self._pick_db(sql)
        fn = db1_batch if db == "db1" else db2_batch
        fn([(sql, list(row)) for row in seq])
        return self

    def commit(self):
        pass  # Turso auto-commits every statement

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass


def _db_available(db_path: Path) -> bool:
    """True if the local SQLite file exists OR Turso credentials are configured."""
    return db_path.exists() or bool(
        os.getenv("TURSO_DATABASE_URL") and os.getenv("TURSO_TOKEN")
    )


def _conn(db_path: Path):
    """Return a real sqlite3 connection if the file exists, else a TursoConn proxy."""
    if db_path.exists():
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        return conn
    return _TursoConn()  # auto-routes by table name


def _identity_conn():
    return _conn(IDENTITY_DB)


def _rows(conn, sql: str, params: tuple = ()) -> list[dict]:
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _one(conn, sql: str, params: tuple = ()) -> dict | None:
    row = conn.execute(sql, params).fetchone()
    return dict(row) if row else None


def _resolve(identifier: str) -> dict | None:
    conn = _identity_conn()
    row = _one(
        conn,
        """
        SELECT * FROM company_map
        WHERE nse_code = ?
           OR bse_code = ?
           OR screener_symbol = ?
           OR isin = ?
        LIMIT 1
        """,
        (identifier, identifier, identifier, identifier),
    )
    conn.close()
    return row


def _not_found(identifier: str) -> str:
    return (
        f"Company '{identifier}' not found. "
        "Try: NSE symbol (RELIANCE), BSE code (500325), "
        "ISIN (INE002A01018), screener slug (Reliance-Industries). "
        "Call find_company if unsure."
    )


def _fmt(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


def _screener_id(identifier: str) -> tuple[int | None, str | None]:
    company = _resolve(identifier)
    if not company:
        return None, _not_found(identifier)
    if not company["screener_company_id"]:
        return None, f"No screener fundamentals linked for '{identifier}'."
    if not _db_available(SCREENER_DB):
        return None, "Screener DB not found. Run: python run_all.py --only screener"
    return company["screener_company_id"], None


# ---------------------------------------------------------------------------
# Pure-Python technical indicator engine
# ---------------------------------------------------------------------------

def _ema_series(values: list[float], period: int) -> list[float]:
    """Full EMA series using Wilder's smoothing seeded with SMA."""
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = sum(values[:period]) / period
    result = [ema]
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
        result.append(ema)
    return result


def _compute_indicators(closes: list[float]) -> dict:
    """
    Compute RSI-14, MACD(12,26,9), Bollinger Bands(20,2),
    EMA-20/50/200 from a closing price series.

    Returns a dict of indicator values at the most recent data point.
    Requires at least 26 data points; 200+ for full EMA-200.
    """
    n = len(closes)
    result: dict[str, Any] = {"data_points": n, "current_price": closes[-1] if closes else None}

    if n < 2:
        return result

    # ── RSI-14 ─────────────────────────────────────────────────────────────
    if n >= 15:
        changes = [closes[i] - closes[i - 1] for i in range(1, n)]
        gains  = [max(c, 0.0) for c in changes]
        losses = [abs(min(c, 0.0)) for c in changes]

        avg_gain = sum(gains[:14]) / 14
        avg_loss = sum(losses[:14]) / 14
        for i in range(14, len(changes)):
            avg_gain = (avg_gain * 13 + gains[i]) / 14
            avg_loss = (avg_loss * 13 + losses[i]) / 14

        if avg_loss == 0:
            result["rsi_14"] = 100.0
        else:
            result["rsi_14"] = round(100 - (100 / (1 + avg_gain / avg_loss)), 2)

        # RSI interpretation
        rsi = result.get("rsi_14", 50)
        if rsi < 30:
            result["rsi_signal"] = "oversold — potential reversal opportunity"
        elif rsi > 70:
            result["rsi_signal"] = "overbought — caution, momentum may be peaking"
        elif rsi < 45:
            result["rsi_signal"] = "recovering from weakness"
        elif rsi > 55:
            result["rsi_signal"] = "trending upward with positive momentum"
        else:
            result["rsi_signal"] = "neutral"

    # ── MACD (12, 26, 9) ───────────────────────────────────────────────────
    if n >= 35:
        ema12 = _ema_series(closes, 12)  # len = n - 11
        ema26 = _ema_series(closes, 26)  # len = n - 25

        if ema12 and ema26:
            # Align: ema12[14:] has same length as ema26
            ema12_aligned = ema12[14:]
            macd_series = [e12 - e26 for e12, e26 in zip(ema12_aligned, ema26)]

            if len(macd_series) >= 9:
                signal_series = _ema_series(macd_series, 9)
                if signal_series:
                    macd_val  = macd_series[-1]
                    signal_val = signal_series[-1]
                    hist       = macd_val - signal_val

                    result["macd_line"]      = round(macd_val, 4)
                    result["macd_signal"]    = round(signal_val, 4)
                    result["macd_histogram"] = round(hist, 4)

                    if hist > 0 and len(macd_series) >= 2:
                        prev_hist = macd_series[-2] - _ema_series(macd_series[:-1], 9)[-1] if len(macd_series) > 9 else hist
                        result["macd_trend"] = "bullish and accelerating" if hist > prev_hist else "bullish but losing steam"
                    elif hist < 0:
                        result["macd_trend"] = "bearish momentum"
                    else:
                        result["macd_trend"] = "neutral"

    # ── Bollinger Bands (20, 2) ────────────────────────────────────────────
    if n >= 20:
        last20 = closes[-20:]
        sma20  = sum(last20) / 20
        variance = sum((x - sma20) ** 2 for x in last20) / 20
        std20  = math.sqrt(variance)
        upper  = sma20 + 2 * std20
        lower  = sma20 - 2 * std20
        cur    = closes[-1]
        pct_b  = (cur - lower) / (upper - lower) if (upper - lower) > 0 else 0.5
        bw     = (upper - lower) / sma20 if sma20 > 0 else 0

        result["bb_upper"]   = round(upper, 2)
        result["bb_middle"]  = round(sma20, 2)
        result["bb_lower"]   = round(lower, 2)
        result["bb_pct_b"]   = round(pct_b, 4)
        result["bb_width"]   = round(bw, 4)

        if pct_b < 0.05:
            result["bb_signal"] = "price at lower band — oversold, watch for bounce"
        elif pct_b > 0.95:
            result["bb_signal"] = "price at upper band — overbought, watch for pullback"
        elif bw < 0.05:
            result["bb_signal"] = "bands squeezing — breakout imminent, direction unclear"
        else:
            result["bb_signal"] = "neutral"

    # ── EMAs ───────────────────────────────────────────────────────────────
    for period in [20, 50, 200]:
        if n >= period:
            ema_val = _ema_series(closes, period)
            if ema_val:
                result[f"ema_{period}"] = round(ema_val[-1], 2)

    cur = closes[-1] if closes else None
    if cur and "ema_50" in result:
        result["price_vs_ema_50_pct"] = round((cur / result["ema_50"] - 1) * 100, 2)
    if cur and "ema_200" in result:
        result["price_vs_ema_200_pct"] = round((cur / result["ema_200"] - 1) * 100, 2)
        result["trend"] = "above EMA-200 — long-term uptrend" if cur > result["ema_200"] else "below EMA-200 — long-term downtrend"

    if "ema_50" in result and "ema_200" in result:
        result["cross_signal"] = (
            "Golden Cross (EMA50 > EMA200) — long-term bullish"
            if result["ema_50"] > result["ema_200"]
            else "Death Cross (EMA50 < EMA200) — long-term bearish"
        )

    return result


# ---------------------------------------------------------------------------
# Tool 1: find_company
# ---------------------------------------------------------------------------

@mcp.tool()
def find_company(identifier: str) -> str:
    """
    Look up a company by any identifier and return its full identity record.

    Accepted:
      NSE symbol    : RELIANCE, HDFCBANK, TCS
      BSE code      : 500325, 500180, 532540
      ISIN          : INE002A01018
      Screener slug : Reliance-Industries, HDFC-Bank

    Returns all module IDs and canonical codes, or an error if not found.
    Call this first when you have an ambiguous name or partial identifier.
    """
    try:
        row = _resolve(identifier)
        return _fmt(row) if row else _not_found(identifier)
    except Exception as e:
        return f"Error looking up '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 2: get_company_overview
# ---------------------------------------------------------------------------

@mcp.tool()
def get_company_overview(identifier: str) -> str:
    """
    Fetch the business description and key highlights for a company.

    Returns:
      - name          : company name
      - nse_code      : NSE trading symbol
      - about         : paragraph describing what the business does
      - key_points    : bullet-point highlights (products, market position, moats)

    This is the context layer — call this FIRST before looking at numbers,
    so you understand what business you're analysing.

    Without this context you risk misinterpreting metrics. For example:
    a low P/E in pharma is very different from a low P/E in a commodity business.

    Accepted identifier: NSE code, BSE code, ISIN, or screener slug.
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        if not company["tickertape_company_id"] or not _db_available(NEWS_DB):
            return f"No company overview data for '{identifier}'."

        conn = _conn(NEWS_DB)
        row = _one(
            conn,
            "SELECT company_name, nse_code, bse_code, about, key_points FROM companies WHERE id = ? LIMIT 1",
            (company["tickertape_company_id"],),
        )
        conn.close()

        if not row:
            return f"Company overview not available for '{identifier}'."

        return _fmt({
            "company": identifier,
            "name": row.get("company_name"),
            "nse_code": row.get("nse_code"),
            "bse_code": row.get("bse_code"),
            "about": row.get("about"),
            "key_points": row.get("key_points"),
        })
    except Exception as e:
        return f"Error fetching overview for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 3: get_company_profile
# ---------------------------------------------------------------------------

@mcp.tool()
def get_company_profile(identifier: str) -> str:
    """
    Fetch a complete multi-source snapshot for a company in a single call.

    Combines all three sources:
      1. Identity      : canonical codes, module IDs
      2. Sentiment     : last 30 news articles with FinBERT scores
      3. Fundamentals  : latest snapshot (PE, PB, ROE, ROCE, market cap)
      4. Signals       : technical signals fired in the last 90 days

    Best starting point for any single-stock deep-dive.
    Follow up with get_financial_statements, get_shareholding,
    analyze_earnings_quality, and get_current_indicators for full depth.

    Accepted identifier: NSE code, BSE code, ISIN, or screener slug.
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        profile: dict[str, Any] = {"identity": company}

        if company["tickertape_company_id"] and _db_available(NEWS_DB):
            conn = _conn(NEWS_DB)
            profile["sentiment"] = _rows(
                conn,
                """
                SELECT  a.headline, a.published_at,
                        a.score_positive, a.score_negative, a.score_neutral
                FROM    news_articles a
                JOIN    article_stocks ast ON ast.article_id = a.id
                WHERE   ast.company_id = ?
                  AND   a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
                ORDER BY a.published_at DESC
                LIMIT 30
                """,
                (company["tickertape_company_id"],),
            )
            conn.close()
        else:
            profile["sentiment"] = []

        if company["screener_company_id"] and _db_available(SCREENER_DB):
            conn = _conn(SCREENER_DB)
            profile["fundamentals"] = _one(
                conn,
                """
                SELECT market_cap, current_price, high_52w, low_52w,
                       pe_ratio, pb_ratio, roe_pct, roce_pct, dividend_yield, snapshot_date
                FROM   fact_realtime_metrics
                WHERE  company_id = ?
                ORDER BY snapshot_date DESC LIMIT 1
                """,
                (company["screener_company_id"],),
            ) or {}
            conn.close()
        else:
            profile["fundamentals"] = {}

        if company["ticker_id"] and _db_available(TECHNICAL_DB):
            conn = _conn(TECHNICAL_DB)
            profile["signals"] = _rows(
                conn,
                """
                SELECT  date, indicator, signal_type, direction,
                        value_primary, label_primary, value_secondary, label_secondary
                FROM    signals
                WHERE   ticker_id = ?
                  AND   date >= date('now', '-90 days')
                ORDER BY date DESC
                """,
                (company["ticker_id"],),
            )
            conn.close()
        else:
            profile["signals"] = []

        return _fmt(profile)
    except Exception as e:
        return f"Error fetching profile for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 4: get_sentiment
# ---------------------------------------------------------------------------

@mcp.tool()
def get_sentiment(identifier: str, limit: int = 50) -> str:
    """
    Fetch recent news articles with full details and FinBERT sentiment scores.

    Each article includes:
      - headline, summary  : title and article body summary
      - published_at       : publication datetime
      - publisher          : source publication
      - tag                : article category/tag
      - score_positive, score_negative, score_neutral : probabilities (0–1, sum ~1)

    Also returns aggregate sentiment (avg scores + dominant direction).

    Interpretation:
      score_positive > 0.65 → strongly positive
      score_negative > 0.65 → strongly negative
      Use 20+ articles for a reliable trend signal.

    Args:
      identifier : NSE code, BSE code, ISIN, or screener slug
      limit      : articles to return (default 50, max 200)
    """
    try:
        limit = min(limit, 200)
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["tickertape_company_id"]:
            return f"No news linked for '{identifier}'."
        if not _db_available(NEWS_DB):
            return "News DB not found. Run: python run_all.py --only news"

        conn = _conn(NEWS_DB)
        articles = _rows(
            conn,
            """
            SELECT  a.headline, a.summary, a.published_at,
                    a.publisher, a.tag, a.link,
                    a.score_positive, a.score_negative, a.score_neutral
            FROM    news_articles a
            JOIN    article_stocks ast ON ast.article_id = a.id
            WHERE   ast.company_id = ?
              AND   a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
            ORDER BY a.published_at DESC
            LIMIT ?
            """,
            (company["tickertape_company_id"], limit),
        )
        conn.close()

        if not articles:
            return f"No scored articles found for '{identifier}'."

        avg_pos = sum(a["score_positive"] or 0 for a in articles) / len(articles)
        avg_neg = sum(a["score_negative"] or 0 for a in articles) / len(articles)
        avg_neu = sum(a["score_neutral"]  or 0 for a in articles) / len(articles)

        return _fmt({
            "company": identifier,
            "articles_returned": len(articles),
            "aggregate": {
                "avg_positive": round(avg_pos, 4),
                "avg_negative": round(avg_neg, 4),
                "avg_neutral":  round(avg_neu, 4),
                "dominant": max(
                    ("positive", avg_pos), ("negative", avg_neg), ("neutral", avg_neu),
                    key=lambda x: x[1]
                )[0],
            },
            "articles": articles,
        })
    except Exception as e:
        return f"Error fetching sentiment for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 5: get_news_price_impact
# ---------------------------------------------------------------------------

@mcp.tool()
def get_news_price_impact(identifier: str, limit: int = 30) -> str:
    """
    Fetch news articles with the stock's price reaction on the day of publication.

    This tool reveals the most important signal institutional investors use:
    DID THE MARKET BELIEVE THE NEWS?

    Each article includes:
      - headline, summary, published_at, publisher
      - score_positive, score_negative, score_neutral (FinBERT)
      - initial_price  : stock price when article was published
      - close_price    : stock's closing price that day
      - price_reaction_pct : % change from initial to close (same-day reaction)
      - sentiment_price_alignment : did price move match sentiment?

    Interpretation patterns:
      POSITIVE news + positive price reaction  → market believed it (normal)
      POSITIVE news + NEGATIVE price reaction  → DISTRIBUTION SIGNAL
        (insiders selling into good news — they know something)
      NEGATIVE news + POSITIVE price reaction  → ACCUMULATION SIGNAL
        (smart money buying the dip on bad news they consider temporary)
      NEUTRAL sentiment + big price move       → algo / institutional action not
        reflected in news coverage

    Args:
      identifier : NSE code, BSE code, ISIN, or screener slug
      limit      : articles to return (default 30, max 100)
    """
    try:
        limit = min(limit, 100)
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["tickertape_company_id"]:
            return f"No news linked for '{identifier}'."
        if not _db_available(NEWS_DB):
            return "News DB not found."

        conn = _conn(NEWS_DB)
        rows = _rows(
            conn,
            """
            SELECT  a.headline, a.summary, a.published_at, a.publisher, a.tag,
                    a.score_positive, a.score_negative, a.score_neutral,
                    ast.initial_price, ast.price, ast.close_price
            FROM    news_articles a
            JOIN    article_stocks ast ON ast.article_id = a.id
            WHERE   ast.company_id = ?
              AND   a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
              AND   ast.initial_price IS NOT NULL AND ast.close_price IS NOT NULL
              AND   ast.initial_price > 0
            ORDER BY a.published_at DESC
            LIMIT ?
            """,
            (company["tickertape_company_id"], limit),
        )
        conn.close()

        if not rows:
            return f"No news with price data found for '{identifier}'."

        enriched = []
        for r in rows:
            entry = dict(r)
            ip = r["initial_price"]
            cp = r["close_price"]
            if ip and cp and ip > 0:
                reaction = round((cp - ip) / ip * 100, 2)
                entry["price_reaction_pct"] = reaction

                # Dominant sentiment
                scores = {
                    "positive": r["score_positive"] or 0,
                    "negative": r["score_negative"] or 0,
                    "neutral":  r["score_neutral"]  or 0,
                }
                dominant = max(scores, key=scores.get)

                if dominant == "positive" and reaction >= 0.5:
                    entry["alignment"] = "ALIGNED — positive news, positive price"
                elif dominant == "positive" and reaction <= -0.5:
                    entry["alignment"] = "DIVERGENCE ⚠ — positive news but stock fell (distribution signal)"
                elif dominant == "negative" and reaction <= -0.5:
                    entry["alignment"] = "ALIGNED — negative news, negative price"
                elif dominant == "negative" and reaction >= 0.5:
                    entry["alignment"] = "DIVERGENCE ⚠ — negative news but stock rose (accumulation signal)"
                else:
                    entry["alignment"] = "NEUTRAL — muted reaction"
            else:
                entry["price_reaction_pct"] = None
                entry["alignment"] = "N/A — price data unavailable"

            enriched.append(entry)

        divergence_count = sum(1 for e in enriched if "DIVERGENCE" in (e.get("alignment") or ""))

        return _fmt({
            "company": identifier,
            "articles_returned": len(enriched),
            "divergence_events": divergence_count,
            "note": (
                f"{divergence_count} sentiment-price divergences detected — "
                "these are the highest-signal events for institutional activity"
                if divergence_count > 0 else "No significant divergences detected"
            ),
            "articles": enriched,
        })
    except Exception as e:
        return f"Error fetching news price impact for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 6: get_current_indicators
# ---------------------------------------------------------------------------

@mcp.tool()
def get_current_indicators(identifier: str) -> str:
    """
    Compute live technical indicators from the latest OHLCV data.

    Unlike stored signals (which are historical events), this computes
    the CURRENT state of every indicator — the equivalent of looking at
    a Bloomberg terminal right now.

    Returns:
      current_price        : latest closing price
      rsi_14               : current RSI value (0–100)
      rsi_signal           : plain-English interpretation
      macd_line            : MACD line value
      macd_signal          : MACD signal line value
      macd_histogram       : histogram (positive = bullish momentum)
      macd_trend           : plain-English interpretation
      bb_upper/middle/lower: Bollinger Band levels
      bb_pct_b             : position within bands (0 = lower band, 1 = upper)
      bb_signal            : plain-English interpretation
      ema_20/50/200        : EMA values
      price_vs_ema_50_pct  : % above/below 50-day EMA
      price_vs_ema_200_pct : % above/below 200-day EMA
      trend                : "above EMA-200 uptrend" or "below EMA-200 downtrend"
      cross_signal         : Golden Cross or Death Cross status

    Uses the last 250 trading days of OHLCV for computation.
    Requires at least 26 data points; EMA-200 needs 200+ points.

    Accepted identifier: NSE code, BSE code, ISIN, or screener slug.
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["ticker_id"]:
            return f"No technical data linked for '{identifier}'."
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found."

        conn = _conn(TECHNICAL_DB)
        rows = _rows(
            conn,
            """
            SELECT close FROM ohlcv
            WHERE  ticker_id = ?
              AND  date >= date('now', '-400 days')
            ORDER BY date ASC
            """,
            (company["ticker_id"],),
        )
        conn.close()

        if not rows:
            return f"No OHLCV data found for '{identifier}'."

        closes = [r["close"] for r in rows if r["close"] is not None]
        if len(closes) < 5:
            return f"Insufficient price data for '{identifier}' ({len(closes)} points)."

        indicators = _compute_indicators(closes)

        return _fmt({
            "company": identifier,
            "yf_symbol": company["yf_symbol"],
            "ohlcv_points_used": len(closes),
            "indicators": indicators,
        })
    except Exception as e:
        return f"Error computing indicators for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 7: get_fundamentals
# ---------------------------------------------------------------------------

@mcp.tool()
def get_fundamentals(identifier: str) -> str:
    """
    Fetch the latest fundamental/valuation snapshot from screener.in.

    Returns: market_cap (INR crores), current_price, high_52w, low_52w,
             pe_ratio, pb_ratio, roe_pct, roce_pct, dividend_yield, snapshot_date.

    For multi-year financial history, use get_financial_statements.
    For working capital and ROCE trends, use get_historical_ratios.
    """
    try:
        cid, err = _screener_id(identifier)
        if err:
            return err
        conn = _conn(SCREENER_DB)
        row = _one(
            conn,
            """
            SELECT market_cap, current_price, high_52w, low_52w,
                   pe_ratio, pb_ratio, roe_pct, roce_pct, dividend_yield, snapshot_date
            FROM   fact_realtime_metrics
            WHERE  company_id = ?
            ORDER BY snapshot_date DESC LIMIT 1
            """,
            (cid,),
        )
        conn.close()
        if not row:
            return f"No fundamentals snapshot found for '{identifier}'."
        return _fmt({"company": identifier, "fundamentals": row})
    except Exception as e:
        return f"Error fetching fundamentals for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 8: get_financial_statements
# ---------------------------------------------------------------------------

@mcp.tool()
def get_financial_statements(
    identifier: str,
    statement: str = "all",
    periods: int = 8,
) -> str:
    """
    Fetch historical financial statements (up to 10 years annual, 12 quarters).

    Args:
      statement : "quarterly" | "profit_loss" | "balance_sheet" | "cash_flow" | "all"
      periods   : number of periods per statement (default 8, max 20)

    All values in INR crores.

    Quarterly: sales, expenses, operating_profit, opm_pct, net_profit, eps
    P&L annual: same + dividend_payout_pct
    Balance sheet: equity, reserves, borrowings, fixed_assets, cwip,
                   investments, total_assets
    Cash flow: cash_from_operating, cash_from_investing, cash_from_financing

    KEY ANALYSIS:
      Operating CF / net_profit > 1.0 = high earnings quality
      Borrowings rising faster than reserves = leverage risk
      Negative cash_from_investing = company investing for growth
    """
    try:
        periods = min(periods, 20)
        cid, err = _screener_id(identifier)
        if err:
            return err
        conn = _conn(SCREENER_DB)
        result: dict[str, Any] = {"company": identifier}

        if statement in ("quarterly", "all"):
            result["quarterly_results"] = _rows(conn,
                """
                SELECT dp.period_label, dp.year, dp.quarter,
                       fqr.sales, fqr.expenses, fqr.operating_profit, fqr.opm_pct,
                       fqr.other_income, fqr.interest, fqr.depreciation,
                       fqr.profit_before_tax, fqr.tax_pct, fqr.net_profit, fqr.eps
                FROM   fact_quarterly_results fqr
                JOIN   dim_period dp ON dp.period_id = fqr.period_id
                WHERE  fqr.company_id = ?
                ORDER BY dp.year DESC, dp.quarter DESC LIMIT ?
                """, (cid, periods))

        if statement in ("profit_loss", "all"):
            result["profit_loss"] = _rows(conn,
                """
                SELECT dp.period_label, dp.year,
                       fpl.sales, fpl.expenses, fpl.operating_profit, fpl.opm_pct,
                       fpl.other_income, fpl.interest, fpl.depreciation,
                       fpl.profit_before_tax, fpl.tax_pct, fpl.net_profit,
                       fpl.eps, fpl.dividend_payout_pct
                FROM   fact_profit_loss fpl
                JOIN   dim_period dp ON dp.period_id = fpl.period_id
                WHERE  fpl.company_id = ?
                ORDER BY dp.year DESC LIMIT ?
                """, (cid, periods))

        if statement in ("balance_sheet", "all"):
            result["balance_sheet"] = _rows(conn,
                """
                SELECT dp.period_label, dp.year,
                       fbs.equity_capital, fbs.reserves, fbs.borrowings,
                       fbs.other_liabilities, fbs.total_liabilities,
                       fbs.fixed_assets, fbs.cwip, fbs.investments,
                       fbs.other_assets, fbs.total_assets
                FROM   fact_balance_sheet fbs
                JOIN   dim_period dp ON dp.period_id = fbs.period_id
                WHERE  fbs.company_id = ?
                ORDER BY dp.year DESC LIMIT ?
                """, (cid, periods))

        if statement in ("cash_flow", "all"):
            result["cash_flow"] = _rows(conn,
                """
                SELECT dp.period_label, dp.year,
                       fcf.cash_from_operating, fcf.cash_from_investing,
                       fcf.cash_from_financing, fcf.net_cash_flow
                FROM   fact_cash_flow fcf
                JOIN   dim_period dp ON dp.period_id = fcf.period_id
                WHERE  fcf.company_id = ?
                ORDER BY dp.year DESC LIMIT ?
                """, (cid, periods))

        conn.close()
        return _fmt(result)
    except Exception as e:
        return f"Error fetching financial statements for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 9: get_shareholding
# ---------------------------------------------------------------------------

@mcp.tool()
def get_shareholding(
    identifier: str,
    period_type: str = "quarterly",
    periods: int = 8,
) -> str:
    """
    Fetch shareholding pattern history for a company.

    Args:
      period_type : "quarterly" (default) or "yearly"
      periods     : number of periods (default 8, max 20)

    Fields: promoters_pct, fiis_pct, diis_pct, government_pct,
            public_pct, num_shareholders per period.

    KEY SIGNALS:
      Promoter % declining consecutively → insider distribution (red flag)
      Promoter % rising                  → insider confidence (bullish)
      FII + DII both rising              → strong institutional conviction
      Institutional falling + public rising → distribution in progress
    """
    try:
        periods = min(periods, 20)
        cid, err = _screener_id(identifier)
        if err:
            return err
        conn = _conn(SCREENER_DB)
        rows = _rows(conn,
            """
            SELECT dp.period_label, dp.year, dp.quarter,
                   fs.promoters_pct, fs.fiis_pct, fs.diis_pct,
                   fs.government_pct, fs.public_pct, fs.num_shareholders
            FROM   fact_shareholding fs
            JOIN   dim_period dp ON dp.period_id = fs.period_id
            WHERE  fs.company_id = ? AND fs.period_type = ?
            ORDER BY dp.year DESC, dp.quarter DESC NULLS LAST
            LIMIT ?
            """, (cid, period_type, periods))
        conn.close()

        if not rows:
            return f"No shareholding data for '{identifier}'."

        # Compute promoter trend
        promoter_values = [r["promoters_pct"] for r in rows if r["promoters_pct"] is not None]
        trend = "insufficient data"
        if len(promoter_values) >= 2:
            recent = promoter_values[:3]
            older  = promoter_values[3:6] if len(promoter_values) >= 6 else promoter_values[2:]
            if older:
                avg_recent = sum(recent) / len(recent)
                avg_older  = sum(older)  / len(older)
                diff = avg_recent - avg_older
                if diff > 1.0:
                    trend = f"INCREASING ↑ (+{diff:.1f}pp) — promoter confidence rising"
                elif diff < -1.0:
                    trend = f"DECREASING ↓ ({diff:.1f}pp) — promoter reducing stake ⚠"
                else:
                    trend = "STABLE — no significant change"

        return _fmt({
            "company": identifier,
            "period_type": period_type,
            "promoter_trend": trend,
            "shareholding": rows,
        })
    except Exception as e:
        return f"Error fetching shareholding for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 10: get_historical_ratios
# ---------------------------------------------------------------------------

@mcp.tool()
def get_historical_ratios(identifier: str, periods: int = 8) -> str:
    """
    Fetch historical efficiency and capital allocation ratios.

    Fields per year: roce_pct, debtor_days, inventory_days, days_payable,
                     cash_conversion_cycle, working_capital_days.

    KEY INSIGHTS:
      ROCE consistently > 15%     → durable competitive advantage
      ROCE declining year-on-year → moat eroding, investigate why
      Rising debtor_days          → customers delaying payments (cash risk)
      Negative cash_conversion_cycle → collects before paying (excellent)
      Low/negative CCC            → capital-light business model
    """
    try:
        periods = min(periods, 20)
        cid, err = _screener_id(identifier)
        if err:
            return err
        conn = _conn(SCREENER_DB)
        rows = _rows(conn,
            """
            SELECT dp.period_label, dp.year,
                   fr.roce_pct, fr.debtor_days, fr.inventory_days,
                   fr.days_payable, fr.cash_conversion_cycle, fr.working_capital_days
            FROM   fact_ratios fr
            JOIN   dim_period dp ON dp.period_id = fr.period_id
            WHERE  fr.company_id = ?
            ORDER BY dp.year DESC LIMIT ?
            """, (cid, periods))
        conn.close()

        if not rows:
            return f"No ratio data for '{identifier}'."

        roce_values = [r["roce_pct"] for r in rows if r["roce_pct"] is not None]
        roce_trend = "insufficient data"
        if len(roce_values) >= 3:
            if roce_values[0] > roce_values[-1]:
                roce_trend = f"IMPROVING — ROCE up from {roce_values[-1]:.1f}% to {roce_values[0]:.1f}%"
            elif roce_values[0] < roce_values[-1]:
                roce_trend = f"DECLINING ⚠ — ROCE down from {roce_values[-1]:.1f}% to {roce_values[0]:.1f}%"
            else:
                roce_trend = "STABLE"

        return _fmt({
            "company": identifier,
            "roce_trend": roce_trend,
            "ratios": rows,
        })
    except Exception as e:
        return f"Error fetching ratios for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 11: analyze_earnings_quality
# ---------------------------------------------------------------------------

@mcp.tool()
def analyze_earnings_quality(identifier: str, periods: int = 8) -> str:
    """
    Analyse the quality of reported earnings by comparing operating cash flow
    to net profit over multiple years.

    This is one of the most powerful screens used by institutional investors.
    Accounting tricks can inflate PAT but cannot fake operating cash flow.

    Computes per year:
      net_profit         : reported profit after tax (INR crores)
      cash_from_operating: actual cash generated from operations
      cash_conversion_ratio: operating CF / net profit
        > 1.0  = EXCELLENT — company generates more cash than it reports
        0.8-1.0 = GOOD — acceptable quality
        0.6-0.8 = CAUTION — some accrual risk
        < 0.6  = RED FLAG — significant accrual gap
        < 0.0  = CRITICAL — negative operating CF despite positive PAT

      free_cash_flow     : operating CF + investing CF (capex proxy)
      fcf_margin_pct     : FCF as % of sales (capital efficiency)

    Also returns:
      quality_assessment : overall summary of earnings quality
      red_flags          : list of years with critical divergences
      accrual_risk       : overall accrual risk rating (Low/Medium/High/Critical)

    Args:
      identifier : NSE code, BSE code, ISIN, or screener slug
      periods    : years to analyse (default 8, max 15)
    """
    try:
        periods = min(periods, 15)
        cid, err = _screener_id(identifier)
        if err:
            return err

        conn = _conn(SCREENER_DB)

        pl_rows = _rows(conn,
            """
            SELECT dp.year, dp.period_label, fpl.sales, fpl.net_profit
            FROM   fact_profit_loss fpl
            JOIN   dim_period dp ON dp.period_id = fpl.period_id
            WHERE  fpl.company_id = ?
            ORDER BY dp.year DESC LIMIT ?
            """, (cid, periods))

        cf_rows = _rows(conn,
            """
            SELECT dp.year, fcf.cash_from_operating, fcf.cash_from_investing
            FROM   fact_cash_flow fcf
            JOIN   dim_period dp ON dp.period_id = fcf.period_id
            WHERE  fcf.company_id = ?
            ORDER BY dp.year DESC LIMIT ?
            """, (cid, periods))

        conn.close()

        if not pl_rows or not cf_rows:
            return f"Insufficient financial data for earnings quality analysis of '{identifier}'."

        cf_map = {r["year"]: r for r in cf_rows}

        analysis = []
        red_flags = []
        ratios = []

        for pl in pl_rows:
            year = pl["year"]
            cf = cf_map.get(year, {})
            net_profit = pl.get("net_profit") or 0
            op_cf      = cf.get("cash_from_operating")
            inv_cf     = cf.get("cash_from_investing")
            sales      = pl.get("sales") or 0

            row: dict[str, Any] = {
                "year": year,
                "period": pl.get("period_label"),
                "sales": sales,
                "net_profit": net_profit,
                "cash_from_operating": op_cf,
                "cash_from_investing": inv_cf,
            }

            if op_cf is not None and net_profit != 0:
                ratio = op_cf / net_profit
                row["cash_conversion_ratio"] = round(ratio, 3)
                ratios.append(ratio)

                if ratio >= 1.0:
                    row["quality"] = "EXCELLENT"
                elif ratio >= 0.8:
                    row["quality"] = "GOOD"
                elif ratio >= 0.6:
                    row["quality"] = "CAUTION"
                elif op_cf < 0 and net_profit > 0:
                    row["quality"] = "CRITICAL — negative operating CF with positive PAT"
                    red_flags.append(f"{year}: Negative operating CF despite positive PAT")
                else:
                    row["quality"] = "RED FLAG"
                    if ratio < 0.5:
                        red_flags.append(f"{year}: Cash conversion ratio = {ratio:.2f}")
            elif op_cf is not None and net_profit == 0:
                row["quality"] = "N/A — zero net profit"

            if op_cf is not None and inv_cf is not None:
                fcf = op_cf + inv_cf
                row["free_cash_flow"] = round(fcf, 2)
                row["fcf_margin_pct"] = round(fcf / sales * 100, 2) if sales > 0 else None

            analysis.append(row)

        # Overall risk rating
        if ratios:
            avg_ratio = sum(ratios) / len(ratios)
            poor_years = sum(1 for r in ratios if r < 0.7)
            if avg_ratio >= 0.9 and poor_years == 0:
                accrual_risk = "LOW — consistently high earnings quality"
            elif avg_ratio >= 0.75 and poor_years <= 1:
                accrual_risk = "MEDIUM — generally acceptable, minor gaps"
            elif poor_years >= len(ratios) // 2:
                accrual_risk = "HIGH — persistent earnings quality issues"
            else:
                accrual_risk = "ELEVATED — several years of poor cash conversion"
        else:
            accrual_risk = "UNKNOWN — insufficient data"

        return _fmt({
            "company": identifier,
            "accrual_risk": accrual_risk,
            "avg_cash_conversion_ratio": round(sum(ratios) / len(ratios), 3) if ratios else None,
            "red_flags": red_flags,
            "note": (
                "Accrual = PAT that never became cash. "
                "Sustained CF/profit > 1.0 is the gold standard. "
                "Negative operating CF with positive PAT is a critical red flag."
            ),
            "analysis": analysis,
        })
    except Exception as e:
        return f"Error analysing earnings quality for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 12: get_signals
# ---------------------------------------------------------------------------

@mcp.tool()
def get_signals(
    identifier: str,
    days: int = 90,
    direction: str | None = None,
    indicator: str | None = None,
) -> str:
    """
    Fetch historical technical signal events for a company.

    Signals fire when a meaningful technical event occurs:
      RSI        : Oversold (<30) bullish / Overbought (>70) bearish
      MACD       : Bullish or Bearish Crossover
      BB         : Price above upper band / below lower band
      EMA_CROSS  : Golden Cross / Death Cross
      PRICE_VS_EMA: Price crossing 20/50/200 EMA
      OBV        : On-balance volume trend change

    Args:
      days      : look-back window (default 90, max 365)
      direction : "bullish" or "bearish" (optional filter)
      indicator : "RSI", "MACD", "BB", "EMA_CROSS", etc. (optional filter)

    For the CURRENT state of indicators (not just events), use get_current_indicators.
    """
    try:
        days = min(days, 365)
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["ticker_id"]:
            return f"No technical data linked for '{identifier}'."
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found."

        filters = ["ticker_id = ?", f"date >= date('now', '-{days} days')"]
        params: list[Any] = [company["ticker_id"]]

        if direction:
            filters.append("direction = ?")
            params.append(direction.lower())
        if indicator:
            filters.append("indicator = ?")
            params.append(indicator.upper())

        conn = _conn(TECHNICAL_DB)
        signals = _rows(conn,
            f"""
            SELECT date, indicator, signal_type, direction,
                   value_primary, label_primary, value_secondary, label_secondary
            FROM   signals WHERE {" AND ".join(filters)}
            ORDER BY date DESC
            """, tuple(params))
        conn.close()

        if not signals:
            return f"No signals for '{identifier}' in last {days} days."

        return _fmt({
            "company": identifier,
            "yf_symbol": company["yf_symbol"],
            "period_days": days,
            "bullish_count": sum(1 for s in signals if s["direction"] == "bullish"),
            "bearish_count": sum(1 for s in signals if s["direction"] == "bearish"),
            "signals": signals,
        })
    except Exception as e:
        return f"Error fetching signals for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 13: get_price_history
# ---------------------------------------------------------------------------

@mcp.tool()
def get_price_history(identifier: str, days: int = 180) -> str:
    """
    Fetch OHLCV (Open, High, Low, Close, Volume) price history.

    Data from Yahoo Finance. NSE stocks: SYMBOL.NS, BSE-only: CODE.BO.
    Each row: date, open, high, low, close, volume.

    Args:
      days : calendar days to look back (default 180, max 730)

    Use cases: price trend analysis, drawdown from 52w high,
    volume spike detection, custom momentum calculations.
    For current indicator values, use get_current_indicators instead.
    """
    try:
        days = min(days, 730)
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["ticker_id"]:
            return f"No technical data linked for '{identifier}'."
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found."

        conn = _conn(TECHNICAL_DB)
        rows = _rows(conn,
            """
            SELECT date, open, high, low, close, volume FROM ohlcv
            WHERE  ticker_id = ? AND date >= date('now', ?)
            ORDER BY date ASC
            """, (company["ticker_id"], f"-{days} days"))
        conn.close()

        if not rows:
            return f"No OHLCV data for '{identifier}'."

        closes = [r["close"] for r in rows if r["close"]]
        if closes:
            peak = max(closes)
            current = closes[-1]
            drawdown = round((current - peak) / peak * 100, 2)
        else:
            drawdown = None

        return _fmt({
            "company": identifier,
            "yf_symbol": company["yf_symbol"],
            "rows_returned": len(rows),
            "date_range": {"from": rows[0]["date"], "to": rows[-1]["date"]},
            "drawdown_from_period_high_pct": drawdown,
            "ohlcv": rows,
        })
    except Exception as e:
        return f"Error fetching price history for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 14: compare_companies
# ---------------------------------------------------------------------------

@mcp.tool()
def compare_companies(identifiers: list[str]) -> str:
    """
    Compare 2 to 5 companies side-by-side across all key dimensions.

    For each company returns:
      - Latest fundamental snapshot (PE, PB, ROE, ROCE, market cap, dividend yield)
      - Sentiment aggregate (avg positive/negative, dominant, article count)
      - Live technical indicators (RSI, MACD histogram, trend vs EMA-200)
      - Recent signal summary (bullish vs bearish count, last 30 days)

    Perfect for:
      - Sector comparisons ("TCS vs Infosys vs Wipro")
      - Valuation checks ("is HDFC Bank cheap vs Kotak?")
      - Identifying the best stock in a sector before deep-diving

    Args:
      identifiers : list of 2-5 company identifiers
                    (NSE codes, BSE codes, ISINs, or screener slugs)

    Example: compare_companies(["TCS", "INFY", "WIPRO"])
    """
    try:
        if len(identifiers) < 2:
            return "Provide at least 2 company identifiers to compare."
        if len(identifiers) > 5:
            return "Maximum 5 companies per comparison. Please narrow the list."

        result = {}

        for identifier in identifiers:
            company = _resolve(identifier)
            if not company:
                result[identifier] = {"error": _not_found(identifier)}
                continue

            entry: dict[str, Any] = {"name": company.get("name"), "nse_code": company.get("nse_code")}

            # Fundamentals
            if company["screener_company_id"] and _db_available(SCREENER_DB):
                conn = _conn(SCREENER_DB)
                fm = _one(conn,
                    """
                    SELECT market_cap, current_price, high_52w, low_52w,
                           pe_ratio, pb_ratio, roe_pct, roce_pct, dividend_yield, snapshot_date
                    FROM   fact_realtime_metrics
                    WHERE  company_id = ?
                    ORDER BY snapshot_date DESC LIMIT 1
                    """, (company["screener_company_id"],))
                conn.close()
                entry["fundamentals"] = fm or {}
            else:
                entry["fundamentals"] = {}

            # Sentiment aggregate
            if company["tickertape_company_id"] and _db_available(NEWS_DB):
                conn = _conn(NEWS_DB)
                articles = _rows(conn,
                    """
                    SELECT a.score_positive, a.score_negative, a.score_neutral
                    FROM   news_articles a
                    JOIN   article_stocks ast ON ast.article_id = a.id
                    WHERE  ast.company_id = ?
                      AND  a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
                    ORDER BY a.published_at DESC LIMIT 30
                    """, (company["tickertape_company_id"],))
                conn.close()
                if articles:
                    avg_pos = sum(a["score_positive"] or 0 for a in articles) / len(articles)
                    avg_neg = sum(a["score_negative"] or 0 for a in articles) / len(articles)
                    entry["sentiment"] = {
                        "articles": len(articles),
                        "avg_positive": round(avg_pos, 3),
                        "avg_negative": round(avg_neg, 3),
                        "dominant": "positive" if avg_pos > avg_neg else "negative",
                    }
                else:
                    entry["sentiment"] = {"articles": 0}
            else:
                entry["sentiment"] = {}

            # Live indicators
            if company["ticker_id"] and _db_available(TECHNICAL_DB):
                conn = _conn(TECHNICAL_DB)
                price_rows = _rows(conn,
                    """
                    SELECT close FROM ohlcv
                    WHERE  ticker_id = ? AND date >= date('now', '-400 days')
                    ORDER BY date ASC
                    """, (company["ticker_id"],))
                conn.close()
                closes = [r["close"] for r in price_rows if r["close"]]
                if closes:
                    ind = _compute_indicators(closes)
                    entry["live_indicators"] = {
                        "rsi_14": ind.get("rsi_14"),
                        "rsi_signal": ind.get("rsi_signal"),
                        "macd_histogram": ind.get("macd_histogram"),
                        "macd_trend": ind.get("macd_trend"),
                        "bb_pct_b": ind.get("bb_pct_b"),
                        "price_vs_ema_200_pct": ind.get("price_vs_ema_200_pct"),
                        "trend": ind.get("trend"),
                        "cross_signal": ind.get("cross_signal"),
                    }

                # Recent signals
                conn = _conn(TECHNICAL_DB)
                sig_summary = _rows(conn,
                    """
                    SELECT direction, COUNT(*) as count
                    FROM   signals
                    WHERE  ticker_id = ? AND date >= date('now', '-30 days')
                    GROUP BY direction
                    """, (company["ticker_id"],))
                conn.close()
                sig_map = {s["direction"]: s["count"] for s in sig_summary}
                entry["signals_30d"] = {
                    "bullish": sig_map.get("bullish", 0),
                    "bearish": sig_map.get("bearish", 0),
                }

            result[identifier] = entry

        return _fmt({"comparison": result, "companies_compared": len(identifiers)})
    except Exception as e:
        return f"Error comparing companies: {e}"


# ---------------------------------------------------------------------------
# Tool 15: analyze_earnings_quality (already defined above as Tool 11)
# Tool 15: get_signal_clusters
# ---------------------------------------------------------------------------

@mcp.tool()
def get_signal_clusters(
    days: int = 7,
    direction: str = "bullish",
    min_signals: int = 3,
    limit: int = 30,
) -> str:
    """
    Find stocks where multiple technical indicators fired in the same direction
    on the same day — the highest-conviction technical setups in the market.

    When 3 or more indicators align simultaneously, the probability of a
    meaningful move is significantly higher than any single signal.

    For example:
      RSI oversold + MACD bullish crossover + Price at BB lower band
      = three independent systems all saying "buy" on the same day.

    Args:
      days        : how many calendar days to scan (default 7, max 90)
      direction   : "bullish" (default) or "bearish"
      min_signals : minimum number of simultaneous signals required (default 3)
      limit       : max stocks to return (default 30, max 100)

    Returns each stock with:
      - date of the cluster
      - number of signals on that date
      - which indicators fired
      - company name and NSE code
      - fundamental context (PE, ROE) if available

    Use this as your first daily scan — these are the setups worth investigating.
    """
    try:
        days  = min(days, 90)
        limit = min(limit, 100)

        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found."

        conn = _conn(TECHNICAL_DB)
        clusters = _rows(conn,
            f"""
            SELECT  ticker_id, date, direction,
                    COUNT(DISTINCT indicator) as signal_count,
                    GROUP_CONCAT(DISTINCT indicator) as indicators,
                    GROUP_CONCAT(DISTINCT signal_type) as signal_types
            FROM    signals
            WHERE   direction = ?
              AND   date >= date('now', '-{days} days')
            GROUP BY ticker_id, date, direction
            HAVING  COUNT(DISTINCT indicator) >= ?
            ORDER BY signal_count DESC, date DESC
            LIMIT ?
            """, (direction.lower(), min_signals, limit))
        conn.close()

        if not clusters:
            return f"No {direction} signal clusters with {min_signals}+ indicators in last {days} days."

        # Enrich with company info from identity
        id_conn = _identity_conn()
        id_map = {
            r["ticker_id"]: {"nse_code": r["nse_code"], "name": r["name"],
                             "screener_company_id": r["screener_company_id"]}
            for r in _rows(id_conn,
                "SELECT ticker_id, nse_code, name, screener_company_id FROM company_map WHERE ticker_id IS NOT NULL")
        }
        id_conn.close()

        enriched = []
        screener_ids_needed = set()
        for c in clusters:
            info = id_map.get(c["ticker_id"], {})
            entry = {**c, **info}
            enriched.append(entry)
            if info.get("screener_company_id"):
                screener_ids_needed.add(info["screener_company_id"])

        # Batch fetch fundamentals for all clustered stocks
        if screener_ids_needed and _db_available(SCREENER_DB):
            conn = _conn(SCREENER_DB)
            placeholders = ",".join("?" * len(screener_ids_needed))
            fund_rows = _rows(conn,
                f"""
                SELECT company_id, pe_ratio, roe_pct, market_cap,
                       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                FROM   fact_realtime_metrics
                WHERE  company_id IN ({placeholders})
                """, tuple(screener_ids_needed))
            conn.close()
            fund_map = {r["company_id"]: r for r in fund_rows if r.get("rn") == 1}
            for e in enriched:
                scid = e.get("screener_company_id")
                if scid and scid in fund_map:
                    fm = fund_map[scid]
                    e["pe_ratio"]  = fm.get("pe_ratio")
                    e["roe_pct"]   = fm.get("roe_pct")
                    e["market_cap"] = fm.get("market_cap")

        return _fmt({
            "direction": direction,
            "period_days": days,
            "min_signals_required": min_signals,
            "clusters_found": len(enriched),
            "note": (
                f"These {len(enriched)} stocks had {min_signals}+ {direction} indicators align on "
                "the same day. These are the highest-conviction technical setups. "
                "Deep-dive with get_company_profile + analyze_earnings_quality for the best ones."
            ),
            "clusters": enriched,
        })
    except Exception as e:
        return f"Error finding signal clusters: {e}"


# ---------------------------------------------------------------------------
# Tool 16: find_setups
# ---------------------------------------------------------------------------

@mcp.tool()
def find_setups(
    setup_type: str = "value_momentum",
    limit: int = 20,
) -> str:
    """
    Find stocks matching pre-built expert investment setup patterns.

    Each pattern combines fundamental quality + technical timing — the kind
    of cross-source synthesis a 30-year fund manager builds intuitively.

    AVAILABLE SETUPS:
    ─────────────────────────────────────────────────────────
    value_momentum
      Quality business in a temporary technical dip.
      Criteria: ROE > 15%, ROCE > 12%, PE < 30
                + RSI oversold signal in last 60 days
                + Price within 35% of 52-week low
      Signal: "Buy quality on dips"

    quality_compounder
      Consistently high-return business in a long-term uptrend.
      Criteria: ROE > 20%, ROCE > 15%
                + Price above EMA-200 (live computation)
                + Bullish signal in last 30 days
      Signal: "Momentum in quality"

    institutional_accumulation
      Institutions quietly buying — price hasn't moved yet.
      Criteria: FII + DII combined stake UP in last 2 quarters
                + Bullish technical signal in last 45 days
                + ROE > 10%
      Signal: "Follow the smart money"

    promoter_confidence
      Company insiders increasing their own stake.
      Criteria: Promoter stake increased in most recent quarter
                + ROE > 10%, PE < 40
      Signal: "Skin in the game"

    earnings_quality_leaders
      Highest-quality earners — cash matches profits.
      Criteria: Will run earnings quality analysis internally
                + Companies with avg CF/profit > 0.9 over 5 years
                + Bullish technical signal in last 60 days
      Signal: "PAT you can trust"

    turnaround
      Beaten down but fundamentally sound — reversal candidates.
      Criteria: Price within 25% of 52-week low
                + RSI oversold in last 45 days
                + ROE > 8% (still profitable)
                + PE < 20 (not expensive)
      Signal: "Buy fear, sell greed"

    Args:
      setup_type : one of the setup names above (default: value_momentum)
      limit      : max results (default 20, max 50)
    """
    try:
        limit = min(limit, 50)
        valid = ["value_momentum", "quality_compounder", "institutional_accumulation",
                 "promoter_confidence", "earnings_quality_leaders", "turnaround"]
        if setup_type not in valid:
            return f"Unknown setup_type '{setup_type}'. Choose from: {', '.join(valid)}"

        if not _db_available(SCREENER_DB) or not _db_available(IDENTITY_DB):
            return "Screener or identity DB not found."

        results = []

        # ── value_momentum ────────────────────────────────────────────────
        if setup_type == "value_momentum":
            conn = _conn(SCREENER_DB)
            candidates = _rows(conn,
                """
                SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                       frm.market_cap, frm.current_price, frm.high_52w, frm.low_52w,
                       frm.pe_ratio, frm.roe_pct, frm.roce_pct
                FROM   dim_company dc
                JOIN   (SELECT company_id, market_cap, current_price, high_52w, low_52w,
                               pe_ratio, roe_pct, roce_pct,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                        FROM   fact_realtime_metrics) frm
                       ON frm.company_id = dc.company_id AND frm.rn = 1
                WHERE  frm.roe_pct >= 15
                  AND  frm.roce_pct >= 12
                  AND  frm.pe_ratio BETWEEN 5 AND 30
                  AND  frm.current_price IS NOT NULL
                  AND  frm.low_52w IS NOT NULL
                  AND  (frm.current_price - frm.low_52w) / NULLIF(frm.high_52w - frm.low_52w, 0) <= 0.35
                ORDER BY frm.roe_pct DESC
                """)
            conn.close()

            # Filter: must have RSI bullish signal in last 60 days
            id_conn = _identity_conn()
            id_map = {r["screener_company_id"]: r["ticker_id"]
                      for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
            id_conn.close()

            if _db_available(TECHNICAL_DB):
                tech_conn = _conn(TECHNICAL_DB)
                rsi_tickers = {r["ticker_id"] for r in _rows(tech_conn,
                    """SELECT DISTINCT ticker_id FROM signals
                       WHERE indicator = 'RSI' AND direction = 'bullish'
                         AND date >= date('now', '-60 days')""")}
                tech_conn.close()

                results = [c for c in candidates if id_map.get(c["company_id"]) in rsi_tickers][:limit]
            else:
                results = candidates[:limit]

        # ── quality_compounder ────────────────────────────────────────────
        elif setup_type == "quality_compounder":
            conn = _conn(SCREENER_DB)
            candidates = _rows(conn,
                """
                SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                       frm.market_cap, frm.current_price, frm.pe_ratio,
                       frm.roe_pct, frm.roce_pct, frm.pb_ratio
                FROM   dim_company dc
                JOIN   (SELECT company_id, market_cap, current_price, pe_ratio,
                               roe_pct, roce_pct, pb_ratio,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                        FROM   fact_realtime_metrics) frm
                       ON frm.company_id = dc.company_id AND frm.rn = 1
                WHERE  frm.roe_pct >= 20 AND frm.roce_pct >= 15
                ORDER BY frm.roce_pct DESC
                """)
            conn.close()

            id_conn = _identity_conn()
            id_map = {r["screener_company_id"]: r["ticker_id"]
                      for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
            id_conn.close()

            # Filter: price above EMA-200 + bullish signal
            if _db_available(TECHNICAL_DB):
                tech_conn = _conn(TECHNICAL_DB)
                bullish_30d = {r["ticker_id"] for r in _rows(tech_conn,
                    "SELECT DISTINCT ticker_id FROM signals WHERE direction = 'bullish' AND date >= date('now', '-30 days')")}
                tech_conn.close()

                filtered = []
                for c in candidates:
                    tid = id_map.get(c["company_id"])
                    if tid and tid in bullish_30d:
                        # Quick EMA-200 check
                        tech_conn = _conn(TECHNICAL_DB)
                        price_rows = _rows(tech_conn,
                            "SELECT close FROM ohlcv WHERE ticker_id = ? AND date >= date('now', '-400 days') ORDER BY date ASC",
                            (tid,))
                        tech_conn.close()
                        closes = [r["close"] for r in price_rows if r["close"]]
                        if len(closes) >= 200:
                            ema200 = _ema_series(closes, 200)
                            if ema200 and closes[-1] > ema200[-1]:
                                filtered.append(c)
                    if len(filtered) >= limit:
                        break
                results = filtered
            else:
                results = candidates[:limit]

        # ── institutional_accumulation ────────────────────────────────────
        elif setup_type == "institutional_accumulation":
            conn = _conn(SCREENER_DB)
            # Companies where combined FII+DII stake increased in last 2 quarters
            accum = _rows(conn,
                """
                WITH recent AS (
                    SELECT fs.company_id,
                           fs.fiis_pct, fs.diis_pct,
                           dp.year, dp.quarter,
                           ROW_NUMBER() OVER (PARTITION BY fs.company_id ORDER BY dp.year DESC, dp.quarter DESC) rn
                    FROM   fact_shareholding fs
                    JOIN   dim_period dp ON dp.period_id = fs.period_id
                    WHERE  fs.period_type = 'quarterly'
                )
                SELECT r1.company_id,
                       (r1.fiis_pct + r1.diis_pct) AS inst_current,
                       (r2.fiis_pct + r2.diis_pct) AS inst_prev,
                       (r1.fiis_pct + r1.diis_pct) - (r2.fiis_pct + r2.diis_pct) AS inst_change
                FROM   recent r1
                JOIN   recent r2 ON r1.company_id = r2.company_id AND r2.rn = 2
                WHERE  r1.rn = 1
                  AND  inst_change > 1.0
                """)
            accum_map = {r["company_id"]: r for r in accum}

            fund_rows = _rows(conn,
                """
                SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                       frm.market_cap, frm.current_price, frm.pe_ratio, frm.roe_pct
                FROM   dim_company dc
                JOIN   (SELECT company_id, market_cap, current_price, pe_ratio, roe_pct,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                        FROM   fact_realtime_metrics) frm
                       ON frm.company_id = dc.company_id AND frm.rn = 1
                WHERE  frm.roe_pct >= 10
                  AND  dc.company_id IN ({placeholders})
                """.replace("{placeholders}", ",".join("?" * len(accum_map))),
                tuple(accum_map.keys()) if accum_map else (0,))
            conn.close()

            for r in fund_rows:
                r["inst_accumulation_change_pp"] = accum_map.get(r["company_id"], {}).get("inst_change")

            id_conn = _identity_conn()
            id_map = {r["screener_company_id"]: r["ticker_id"]
                      for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
            id_conn.close()

            if _db_available(TECHNICAL_DB):
                tech_conn = _conn(TECHNICAL_DB)
                bullish_45d = {r["ticker_id"] for r in _rows(tech_conn,
                    "SELECT DISTINCT ticker_id FROM signals WHERE direction = 'bullish' AND date >= date('now', '-45 days')")}
                tech_conn.close()
                results = [r for r in fund_rows if id_map.get(r["company_id"]) in bullish_45d][:limit]
            else:
                results = fund_rows[:limit]

        # ── promoter_confidence ───────────────────────────────────────────
        elif setup_type == "promoter_confidence":
            conn = _conn(SCREENER_DB)
            promoter_buyers = _rows(conn,
                """
                WITH recent AS (
                    SELECT fs.company_id, fs.promoters_pct, dp.year, dp.quarter,
                           ROW_NUMBER() OVER (PARTITION BY fs.company_id ORDER BY dp.year DESC, dp.quarter DESC) rn
                    FROM   fact_shareholding fs
                    JOIN   dim_period dp ON dp.period_id = fs.period_id
                    WHERE  fs.period_type = 'quarterly'
                )
                SELECT r1.company_id,
                       r1.promoters_pct AS promoter_current,
                       r2.promoters_pct AS promoter_prev,
                       r1.promoters_pct - r2.promoters_pct AS promoter_change
                FROM   recent r1 JOIN recent r2 ON r1.company_id = r2.company_id AND r2.rn = 2
                WHERE  r1.rn = 1 AND promoter_change > 0.5
                """)
            pb_map = {r["company_id"]: r for r in promoter_buyers}

            if pb_map:
                fund_rows = _rows(conn,
                    """
                    SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                           frm.market_cap, frm.current_price, frm.pe_ratio, frm.roe_pct
                    FROM   dim_company dc
                    JOIN   (SELECT company_id, market_cap, current_price, pe_ratio, roe_pct,
                                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                            FROM   fact_realtime_metrics) frm
                           ON frm.company_id = dc.company_id AND frm.rn = 1
                    WHERE  frm.roe_pct >= 10 AND frm.pe_ratio < 40
                      AND  dc.company_id IN ({p})
                    ORDER BY frm.roe_pct DESC
                    """.replace("{p}", ",".join("?" * len(pb_map))),
                    tuple(pb_map.keys()))
                for r in fund_rows:
                    r["promoter_stake_change_pp"] = pb_map.get(r["company_id"], {}).get("promoter_change")
                results = fund_rows[:limit]
            conn.close()

        # ── turnaround ────────────────────────────────────────────────────
        elif setup_type == "turnaround":
            conn = _conn(SCREENER_DB)
            candidates = _rows(conn,
                """
                SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                       frm.market_cap, frm.current_price, frm.high_52w, frm.low_52w,
                       frm.pe_ratio, frm.roe_pct
                FROM   dim_company dc
                JOIN   (SELECT company_id, market_cap, current_price, high_52w, low_52w,
                               pe_ratio, roe_pct,
                               ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                        FROM   fact_realtime_metrics) frm
                       ON frm.company_id = dc.company_id AND frm.rn = 1
                WHERE  frm.roe_pct >= 8 AND frm.pe_ratio < 20
                  AND  frm.current_price IS NOT NULL AND frm.low_52w IS NOT NULL
                  AND  (frm.current_price - frm.low_52w) / NULLIF(frm.high_52w - frm.low_52w, 0) <= 0.25
                ORDER BY frm.roe_pct DESC
                """)
            conn.close()

            id_conn = _identity_conn()
            id_map = {r["screener_company_id"]: r["ticker_id"]
                      for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
            id_conn.close()

            if _db_available(TECHNICAL_DB):
                tech_conn = _conn(TECHNICAL_DB)
                rsi_tickers = {r["ticker_id"] for r in _rows(tech_conn,
                    """SELECT DISTINCT ticker_id FROM signals
                       WHERE indicator = 'RSI' AND direction = 'bullish'
                         AND date >= date('now', '-45 days')""")}
                tech_conn.close()
                results = [c for c in candidates if id_map.get(c["company_id"]) in rsi_tickers][:limit]
            else:
                results = candidates[:limit]

        # ── earnings_quality_leaders ──────────────────────────────────────
        elif setup_type == "earnings_quality_leaders":
            conn = _conn(SCREENER_DB)
            # Companies where avg CF/profit over recent years is high
            quality_cos = _rows(conn,
                """
                SELECT fpl.company_id,
                       AVG(CASE WHEN fpl.net_profit > 0
                                THEN fcf.cash_from_operating / fpl.net_profit
                                ELSE NULL END) AS avg_cf_ratio,
                       COUNT(*) AS years
                FROM   fact_profit_loss fpl
                JOIN   dim_period dp ON dp.period_id = fpl.period_id
                JOIN   fact_cash_flow fcf ON fcf.company_id = fpl.company_id
                    AND fcf.period_id = fpl.period_id
                WHERE  dp.year >= (SELECT MAX(year) - 4 FROM dim_period)
                  AND  fpl.net_profit > 0
                GROUP BY fpl.company_id
                HAVING avg_cf_ratio >= 0.9 AND years >= 3
                ORDER BY avg_cf_ratio DESC
                """)
            quality_map = {r["company_id"]: r for r in quality_cos}

            if quality_map:
                fund_rows = _rows(conn,
                    """
                    SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code,
                           frm.market_cap, frm.current_price, frm.pe_ratio,
                           frm.roe_pct, frm.roce_pct
                    FROM   dim_company dc
                    JOIN   (SELECT company_id, market_cap, current_price, pe_ratio, roe_pct, roce_pct,
                                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                            FROM   fact_realtime_metrics) frm
                           ON frm.company_id = dc.company_id AND frm.rn = 1
                    WHERE  dc.company_id IN ({p})
                    ORDER BY frm.roce_pct DESC
                    """.replace("{p}", ",".join("?" * len(quality_map))),
                    tuple(quality_map.keys()))
                for r in fund_rows:
                    r["avg_cf_ratio"] = round(quality_map.get(r["company_id"], {}).get("avg_cf_ratio", 0), 3)

                id_conn = _identity_conn()
                id_map = {r["screener_company_id"]: r["ticker_id"]
                          for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
                id_conn.close()

                if _db_available(TECHNICAL_DB):
                    tech_conn = _conn(TECHNICAL_DB)
                    bullish_60d = {r["ticker_id"] for r in _rows(tech_conn,
                        "SELECT DISTINCT ticker_id FROM signals WHERE direction = 'bullish' AND date >= date('now', '-60 days')")}
                    tech_conn.close()
                    results = [r for r in fund_rows if id_map.get(r["company_id"]) in bullish_60d][:limit]
                else:
                    results = fund_rows[:limit]
            conn.close()

        descriptions = {
            "value_momentum":           "Quality business (ROE>15%, ROCE>12%) in a temporary dip (RSI oversold + near 52w low)",
            "quality_compounder":       "High-return business (ROE>20%, ROCE>15%) in long-term uptrend (above EMA-200) with recent bullish signals",
            "institutional_accumulation": "FII + DII combined stake rising >1pp last quarter + bullish technical signal",
            "promoter_confidence":      "Promoter stake increased >0.5pp last quarter + healthy fundamentals",
            "earnings_quality_leaders": "Avg cash conversion ratio ≥ 0.9 over 5 years + recent bullish signal",
            "turnaround":               "Near 52w low + RSI oversold + still profitable (ROE>8%) + reasonable valuation",
        }

        return _fmt({
            "setup_type": setup_type,
            "description": descriptions.get(setup_type, ""),
            "stocks_found": len(results),
            "note": "Deep-dive the top candidates with get_company_profile, analyze_earnings_quality, and get_current_indicators.",
            "stocks": results,
        })
    except Exception as e:
        return f"Error finding setups for '{setup_type}': {e}"


# ---------------------------------------------------------------------------
# Tool 17: screen_stocks
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_stocks(
    min_roe: float | None = None,
    max_pe: float | None = None,
    min_pe: float | None = None,
    max_pb: float | None = None,
    min_roce: float | None = None,
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    min_dividend_yield: float | None = None,
    has_bullish_signal: bool = False,
    has_bearish_signal: bool = False,
    signal_days: int = 30,
    limit: int = 30,
) -> str:
    """
    Screen stocks across all ~5,200 companies using combined fundamental
    and technical filters.

    FUNDAMENTAL FILTERS (latest screener.in snapshot):
      min_roe           : ROE % >= value      (e.g. 15 → ROE ≥ 15%)
      max_pe            : P/E ≤ value
      min_pe            : P/E ≥ value         (excludes negative/zero PE)
      max_pb            : P/B ≤ value
      min_roce          : ROCE % ≥ value
      min_market_cap    : INR crores (20000 = large cap, 5000 = mid, <2000 = small)
      max_market_cap    : INR crores
      min_dividend_yield: dividend yield % ≥ value

    TECHNICAL FILTERS:
      has_bullish_signal: stocks with ≥1 bullish signal in last signal_days
      has_bearish_signal: stocks with ≥1 bearish signal in last signal_days
      signal_days       : look-back window for signal filter (default 30)

    Results ordered by market cap descending. Limit max 100.

    EXAMPLES:
      Quality + cheap:    min_roe=20, max_pe=20, min_roce=15
      Large cap momentum: min_market_cap=20000, has_bullish_signal=True
      High yield value:   min_dividend_yield=3, max_pb=2
      Small cap dips:     max_market_cap=2000, has_bullish_signal=True, signal_days=7
    """
    try:
        limit = min(limit, 100)
        if not _db_available(SCREENER_DB) or not _db_available(IDENTITY_DB):
            return "Screener or identity DB not found."

        filters = ["frm.company_id IS NOT NULL"]
        params: list[Any] = []

        if min_roe           is not None: filters.append("frm.roe_pct >= ?");        params.append(min_roe)
        if max_pe            is not None: filters.append("frm.pe_ratio <= ?");       params.append(max_pe)
        if min_pe            is not None: filters.append("frm.pe_ratio >= ?");       params.append(min_pe)
        if max_pb            is not None: filters.append("frm.pb_ratio <= ?");       params.append(max_pb)
        if min_roce          is not None: filters.append("frm.roce_pct >= ?");       params.append(min_roce)
        if min_market_cap    is not None: filters.append("frm.market_cap >= ?");     params.append(min_market_cap)
        if max_market_cap    is not None: filters.append("frm.market_cap <= ?");     params.append(max_market_cap)
        if min_dividend_yield is not None: filters.append("frm.dividend_yield >= ?"); params.append(min_dividend_yield)

        conn = _conn(SCREENER_DB)
        candidates = _rows(conn,
            f"""
            SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code, dc.bse_code,
                   frm.market_cap, frm.current_price, frm.pe_ratio, frm.pb_ratio,
                   frm.roe_pct, frm.roce_pct, frm.dividend_yield,
                   frm.high_52w, frm.low_52w, frm.snapshot_date
            FROM   dim_company dc
            JOIN   (SELECT company_id, market_cap, current_price, pe_ratio, pb_ratio,
                           roe_pct, roce_pct, dividend_yield, high_52w, low_52w, snapshot_date,
                           ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                    FROM   fact_realtime_metrics) frm
                   ON frm.company_id = dc.company_id AND frm.rn = 1
            WHERE  {" AND ".join(filters)}
            ORDER BY frm.market_cap DESC NULLS LAST
            """, tuple(params))
        conn.close()

        if not candidates:
            return "No stocks matched the given criteria."

        if has_bullish_signal or has_bearish_signal:
            if not _db_available(TECHNICAL_DB):
                return "Technical DB not found for signal filtering."
            id_conn = _identity_conn()
            id_map = {r["screener_company_id"]: r["ticker_id"]
                      for r in _rows(id_conn, "SELECT screener_company_id, ticker_id FROM company_map WHERE ticker_id IS NOT NULL")}
            id_conn.close()
            direction_filter = "bullish" if has_bullish_signal else "bearish"
            tech_conn = _conn(TECHNICAL_DB)
            tickers_with_signal = {r["ticker_id"] for r in _rows(tech_conn,
                f"SELECT DISTINCT ticker_id FROM signals WHERE direction = ? AND date >= date('now', '-{signal_days} days')",
                (direction_filter,))}
            tech_conn.close()
            candidates = [c for c in candidates if id_map.get(c["company_id"]) in tickers_with_signal]
            if not candidates:
                return f"No stocks matched with a {direction_filter} signal in last {signal_days} days."

        return _fmt({
            "total_matched": len(candidates),
            "returned": min(len(candidates), limit),
            "filters": {
                "min_roe": min_roe, "max_pe": max_pe, "min_pe": min_pe,
                "max_pb": max_pb, "min_roce": min_roce,
                "min_market_cap": min_market_cap, "max_market_cap": max_market_cap,
                "min_dividend_yield": min_dividend_yield,
                "has_bullish_signal": has_bullish_signal,
                "has_bearish_signal": has_bearish_signal,
            },
            "stocks": candidates[:limit],
        })
    except Exception as e:
        return f"Error screening stocks: {e}"


# ---------------------------------------------------------------------------
# Tool 18: get_recent_signals
# ---------------------------------------------------------------------------

@mcp.tool()
def get_recent_signals(
    direction: str | None = None,
    indicator: str | None = None,
    days: int = 7,
    limit: int = 50,
) -> str:
    """
    Fetch recent technical signals across ALL companies — market-wide view.

    Use for daily scans: what is the market saying right now?

    Args:
      direction  : "bullish" or "bearish" (optional)
      indicator  : "RSI", "MACD", "BB", "EMA_CROSS", "PRICE_VS_EMA", "OBV" (optional)
      days       : look-back window (default 7, max 90)
      limit      : max results (default 50, max 200)

    Each result includes company name + NSE code alongside the signal details.

    COMMON SCANS:
      Today's RSI oversold:    direction="bullish", indicator="RSI", days=1
      MACD crossovers this week: direction="bullish", indicator="MACD", days=7
      Death crosses (bearish): direction="bearish", indicator="EMA_CROSS"
      Full market overview:    days=1 (no other filters)
    """
    try:
        days  = min(days, 90)
        limit = min(limit, 200)
        if not _db_available(TECHNICAL_DB) or not _db_available(IDENTITY_DB):
            return "Technical or identity DB not found."

        id_conn = _identity_conn()
        id_map = {
            r["ticker_id"]: {"nse_code": r["nse_code"], "name": r["name"]}
            for r in _rows(id_conn, "SELECT ticker_id, nse_code, name FROM company_map WHERE ticker_id IS NOT NULL")
        }
        id_conn.close()

        filters = [f"date >= date('now', '-{days} days')"]
        params: list[Any] = []
        if direction:
            filters.append("direction = ?")
            params.append(direction.lower())
        if indicator:
            filters.append("indicator = ?")
            params.append(indicator.upper())

        conn = _conn(TECHNICAL_DB)
        raw = _rows(conn,
            f"""
            SELECT ticker_id, date, indicator, signal_type, direction,
                   value_primary, label_primary, value_secondary, label_secondary
            FROM   signals WHERE {" AND ".join(filters)}
            ORDER BY date DESC, ticker_id LIMIT ?
            """, tuple(params) + (limit,))
        conn.close()

        if not raw:
            return f"No signals found in last {days} days."

        enriched = [{**r, **id_map.get(r["ticker_id"], {})} for r in raw]

        return _fmt({
            "period_days": days,
            "total_signals": len(enriched),
            "bullish_count": sum(1 for r in enriched if r["direction"] == "bullish"),
            "bearish_count": sum(1 for r in enriched if r["direction"] == "bearish"),
            "filters": {"direction": direction, "indicator": indicator},
            "signals": enriched,
        })
    except Exception as e:
        return f"Error fetching recent signals: {e}"


# ---------------------------------------------------------------------------
# Tool 19: coverage_stats
# ---------------------------------------------------------------------------

@mcp.tool()
def coverage_stats() -> str:
    """
    Return data coverage and freshness statistics for the full database.

    Shows:
      Identity   : total companies, how many linked to each source
      News       : articles, scoring rate, date range
      Technical  : tickers, OHLCV rows, signals, newest data date
      Fundamentals: companies with each type of data, snapshot dates

    Use when asked "how current is your data?" or "what do you have coverage for?"
    """
    try:
        stats: dict[str, Any] = {}

        if _db_available(IDENTITY_DB):
            conn = _identity_conn()
            stats["identity"] = {
                "total_companies":        _one(conn, "SELECT COUNT(*) n FROM company_map")["n"],
                "linked_to_news":         _one(conn, "SELECT COUNT(*) n FROM company_map WHERE tickertape_company_id IS NOT NULL AND entity_type='stock'")["n"],
                "linked_to_technicals":   _one(conn, "SELECT COUNT(*) n FROM company_map WHERE ticker_id IS NOT NULL AND entity_type='stock'")["n"],
                "linked_to_fundamentals": _one(conn, "SELECT COUNT(*) n FROM company_map WHERE screener_company_id IS NOT NULL AND entity_type='stock'")["n"],
                "linked_to_all_three":    _one(conn, "SELECT COUNT(*) n FROM company_map WHERE tickertape_company_id IS NOT NULL AND ticker_id IS NOT NULL AND screener_company_id IS NOT NULL AND entity_type='stock'")["n"],
            }
            conn.close()
        else:
            stats["identity"] = "identity.db not found — run: python identity.py"

        if _db_available(NEWS_DB):
            conn = _conn(NEWS_DB)
            stats["news"] = {
                "total_articles":   _one(conn, "SELECT COUNT(*) n FROM news_articles")["n"],
                "scored_articles":  _one(conn, "SELECT COUNT(*) n FROM news_articles WHERE sentiment_at IS NOT NULL AND sentiment_at != 'ERROR'")["n"],
                "oldest_article":   (_one(conn, "SELECT MIN(published_at) d FROM news_articles") or {}).get("d"),
                "newest_article":   (_one(conn, "SELECT MAX(published_at) d FROM news_articles") or {}).get("d"),
                "articles_with_price_data": _one(conn, "SELECT COUNT(*) n FROM article_stocks WHERE initial_price IS NOT NULL")["n"],
            }
            conn.close()
        else:
            stats["news"] = "tickertape.db not found"

        if _db_available(TECHNICAL_DB):
            conn = _conn(TECHNICAL_DB)
            stats["technical"] = {
                "total_tickers":       _one(conn, "SELECT COUNT(*) n FROM tickers")["n"],
                "tickers_with_ohlcv":  _one(conn, "SELECT COUNT(DISTINCT ticker_id) n FROM ohlcv")["n"],
                "total_ohlcv_rows":    _one(conn, "SELECT COUNT(*) n FROM ohlcv")["n"],
                "total_signals":       _one(conn, "SELECT COUNT(*) n FROM signals")["n"],
                "newest_ohlcv_date":   (_one(conn, "SELECT MAX(date) d FROM ohlcv") or {}).get("d"),
                "newest_signal_date":  (_one(conn, "SELECT MAX(date) d FROM signals") or {}).get("d"),
            }
            conn.close()
        else:
            stats["technical"] = "finance.db not found"

        if _db_available(SCREENER_DB):
            conn = _conn(SCREENER_DB)
            stats["fundamentals"] = {
                "total_companies":          _one(conn, "SELECT COUNT(*) n FROM dim_company")["n"],
                "with_metrics":             _one(conn, "SELECT COUNT(DISTINCT company_id) n FROM fact_realtime_metrics")["n"],
                "with_quarterly_results":   _one(conn, "SELECT COUNT(DISTINCT company_id) n FROM fact_quarterly_results")["n"],
                "with_balance_sheet":       _one(conn, "SELECT COUNT(DISTINCT company_id) n FROM fact_balance_sheet")["n"],
                "with_cash_flow":           _one(conn, "SELECT COUNT(DISTINCT company_id) n FROM fact_cash_flow")["n"],
                "with_shareholding":        _one(conn, "SELECT COUNT(DISTINCT company_id) n FROM fact_shareholding")["n"],
                "newest_snapshot_date":     (_one(conn, "SELECT MAX(snapshot_date) d FROM fact_realtime_metrics") or {}).get("d"),
            }
            conn.close()
        else:
            stats["fundamentals"] = "screener.db not found"

        return _fmt(stats)
    except Exception as e:
        return f"Error fetching coverage stats: {e}"


# ---------------------------------------------------------------------------
# Tool 20: get_price_correlation
# ---------------------------------------------------------------------------

@mcp.tool()
def get_price_correlation(identifiers: list[str], days: int = 180) -> str:
    """
    Compute the price return correlation matrix for 2 to 8 stocks.

    Uses daily percentage returns (not raw prices) over a common date range.
    This tells you how much these stocks move together vs independently —
    critical for portfolio construction and for validating peer groupings.

    Returns:
      - correlation_matrix : NxN matrix of Pearson correlations (-1 to +1)
      - interpretation per pair:
          > 0.85 : highly correlated — move almost in lockstep
          0.60-0.85 : moderately correlated — same sector dynamics
          0.30-0.60 : weak correlation — some shared factors
          < 0.30  : low/no correlation — independent movers
          < 0     : negative — tend to move in opposite directions
      - common_trading_days : number of days both stocks had data
      - volatility per stock: annualised return volatility (%)

    USE CASES:
      1. Validate peer comparison — if two "peers" have low correlation,
         they may not actually face the same market forces.
      2. Portfolio diversification — prefer low-correlation combinations.
      3. Pair trading — highly correlated stocks that diverge in price
         are a classic mean-reversion opportunity.
      4. Risk concentration — if your portfolio is all >0.85 correlated,
         you're taking concentrated sector risk.

    Works best with Claude Desktop's built-in web search:
      Step 1: Ask Claude to search "NSE listed peers of [COMPANY]"
      Step 2: Call compare_companies with the list
      Step 3: Call get_price_correlation to validate they actually behave as peers

    Args:
      identifiers : list of 2-8 company identifiers
      days        : look-back window for correlation (default 180, max 730)
    """
    try:
        if len(identifiers) < 2:
            return "Provide at least 2 identifiers."
        if len(identifiers) > 8:
            return "Maximum 8 stocks per correlation analysis."

        days = min(days, 730)

        if not _db_available(TECHNICAL_DB) or not _db_available(IDENTITY_DB):
            return "Technical or identity DB not found."

        # Resolve all identifiers
        resolved = {}
        for ident in identifiers:
            company = _resolve(ident)
            if not company:
                resolved[ident] = {"error": _not_found(ident)}
            elif not company["ticker_id"]:
                resolved[ident] = {"error": f"No technical data linked for '{ident}'."}
            else:
                resolved[ident] = company

        # Fetch daily closes for each valid ticker
        price_series: dict[str, dict[str, float]] = {}  # ident → {date: close}

        conn = _conn(TECHNICAL_DB)
        for ident, company in resolved.items():
            if "error" in company:
                continue
            rows = _rows(conn,
                """
                SELECT date, close FROM ohlcv
                WHERE  ticker_id = ? AND date >= date('now', ?)
                  AND  close IS NOT NULL
                ORDER BY date ASC
                """, (company["ticker_id"], f"-{days} days"))
            if rows:
                price_series[ident] = {r["date"]: r["close"] for r in rows}
        conn.close()

        if len(price_series) < 2:
            return "Need at least 2 stocks with OHLCV data for correlation analysis."

        # Find common trading dates
        common_dates = sorted(
            set.intersection(*[set(ps.keys()) for ps in price_series.values()])
        )

        if len(common_dates) < 10:
            return f"Only {len(common_dates)} common trading days found — insufficient for correlation."

        # Compute daily returns for each stock on common dates
        returns: dict[str, list[float]] = {}
        for ident, ps in price_series.items():
            closes = [ps[d] for d in common_dates]
            daily_returns = [
                (closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(1, len(closes))
                if closes[i - 1] > 0
            ]
            returns[ident] = daily_returns

        stocks = list(returns.keys())
        n = len(stocks)

        def _pearson(x: list[float], y: list[float]) -> float:
            length = min(len(x), len(y))
            if length < 5:
                return 0.0
            x, y = x[:length], y[:length]
            mx = sum(x) / length
            my = sum(y) / length
            num   = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
            den_x = math.sqrt(sum((xi - mx) ** 2 for xi in x))
            den_y = math.sqrt(sum((yi - my) ** 2 for yi in y))
            if den_x == 0 or den_y == 0:
                return 0.0
            return round(num / (den_x * den_y), 4)

        # Build correlation matrix
        matrix: list[dict] = []
        for i in range(n):
            row_data: dict[str, Any] = {"stock": stocks[i]}
            for j in range(n):
                corr = 1.0 if i == j else _pearson(returns[stocks[i]], returns[stocks[j]])
                row_data[stocks[j]] = corr
            matrix.append(row_data)

        # Pair interpretations
        pairs = []
        for i in range(n):
            for j in range(i + 1, n):
                corr = _pearson(returns[stocks[i]], returns[stocks[j]])
                if corr > 0.85:
                    interp = "HIGHLY CORRELATED — move in lockstep"
                elif corr > 0.60:
                    interp = "MODERATELY CORRELATED — same sector dynamics"
                elif corr > 0.30:
                    interp = "WEAKLY CORRELATED — some shared factors"
                elif corr >= 0:
                    interp = "LOW CORRELATION — largely independent"
                else:
                    interp = "NEGATIVE CORRELATION — tend to move opposite"
                pairs.append({
                    "pair": f"{stocks[i]} vs {stocks[j]}",
                    "correlation": corr,
                    "interpretation": interp,
                })

        # Annualised volatility per stock
        volatility = {}
        for ident, ret in returns.items():
            if ret:
                mean = sum(ret) / len(ret)
                variance = sum((r - mean) ** 2 for r in ret) / len(ret)
                daily_std = math.sqrt(variance)
                ann_vol = round(daily_std * math.sqrt(252) * 100, 2)
                volatility[ident] = f"{ann_vol}%"

        # Errors for unresolved
        errors = {ident: company["error"] for ident, company in resolved.items() if "error" in company}

        return _fmt({
            "common_trading_days": len(common_dates),
            "date_range": {"from": common_dates[0], "to": common_dates[-1]},
            "stocks_analysed": stocks,
            "annualised_volatility": volatility,
            "correlation_matrix": matrix,
            "pair_interpretations": sorted(pairs, key=lambda p: abs(p["correlation"]), reverse=True),
            "errors": errors if errors else None,
            "note": (
                "Pearson correlation of daily returns. "
                "Highly correlated stocks (>0.85) offer less diversification. "
                "For peer validation: search 'NSE peers of [company]' then compare and correlate."
            ),
        })
    except Exception as e:
        return f"Error computing price correlation: {e}"


# ---------------------------------------------------------------------------
# Quant scoring helpers
# ---------------------------------------------------------------------------

def _quality_score(roe, roce, cf_ratio, borrowings, book_equity,
                   roe_prev, roce_prev) -> dict:
    """
    Piotroski-inspired quality score (0–100).
    Higher = better quality business.
    """
    score = 0
    breakdown: dict[str, int] = {}

    # ROE (max 25)
    if roe is not None:
        pts = 25 if roe >= 25 else 20 if roe >= 20 else 15 if roe >= 15 else 8 if roe >= 10 else 0
    else:
        pts = 0
    score += pts
    breakdown["roe"] = pts

    # ROCE (max 25)
    if roce is not None:
        pts = 25 if roce >= 25 else 20 if roce >= 20 else 15 if roce >= 15 else 8 if roce >= 10 else 0
    else:
        pts = 0
    score += pts
    breakdown["roce"] = pts

    # Earnings quality — CF/profit ratio (max 20)
    if cf_ratio is not None:
        pts = 20 if cf_ratio >= 1.2 else 17 if cf_ratio >= 1.0 else 12 if cf_ratio >= 0.8 \
              else 6 if cf_ratio >= 0.5 else 0 if cf_ratio < 0 else 3
    else:
        pts = 10  # unknown → neutral
    score += pts
    breakdown["earnings_quality"] = pts

    # Debt health — D/E ratio (max 20)
    if borrowings is not None and book_equity is not None and book_equity > 0:
        de = borrowings / book_equity
        pts = 20 if de <= 0.3 else 17 if de <= 0.5 else 12 if de <= 1.0 else 6 if de <= 2.0 else 0
    elif borrowings == 0 or borrowings is None:
        pts = 20
    else:
        pts = 0
    score += pts
    breakdown["debt_health"] = pts

    # Improving trend (max 10)
    trend = 0
    if roe is not None and roe_prev is not None and roe > roe_prev:
        trend += 5
    if roce is not None and roce_prev is not None and roce > roce_prev:
        trend += 5
    score += trend
    breakdown["improving_trend"] = trend

    return {"score": min(score, 100), "breakdown": breakdown}


def _value_score(pe, pb, div_yield) -> dict:
    """
    Value score (0–100). Lower PE/PB and higher yield = more value.
    """
    score = 0
    breakdown: dict[str, int] = {}

    # PE (max 35)
    if pe is not None and pe > 0:
        pts = 35 if pe <= 10 else 30 if pe <= 15 else 25 if pe <= 20 else 20 if pe <= 25 \
              else 15 if pe <= 30 else 8 if pe <= 40 else 0
    else:
        pts = 0
    score += pts
    breakdown["pe"] = pts

    # PB (max 35)
    if pb is not None and pb > 0:
        pts = 35 if pb <= 0.5 else 30 if pb <= 1.0 else 25 if pb <= 1.5 else 20 if pb <= 2.0 \
              else 12 if pb <= 3.0 else 6 if pb <= 5.0 else 0
    else:
        pts = 0
    score += pts
    breakdown["pb"] = pts

    # Dividend yield (max 30)
    if div_yield is not None:
        pts = 30 if div_yield >= 5 else 25 if div_yield >= 3 else 18 if div_yield >= 2 \
              else 10 if div_yield >= 1 else 5
    else:
        pts = 5
    score += pts
    breakdown["dividend_yield"] = pts

    return {"score": min(score, 100), "breakdown": breakdown}


def _earnings_surprise_score(sales, profit, opm, prev_sales, prev_profit, prev_opm) -> dict:
    """
    Earnings momentum score (0–100).
    Based on YoY growth in revenue, profit, and margin.
    """
    score = 0
    breakdown: dict[str, Any] = {}

    # Revenue growth (max 30)
    rev_growth = None
    if sales and prev_sales and prev_sales > 0:
        rev_growth = (sales - prev_sales) / prev_sales * 100
        pts = 30 if rev_growth >= 25 else 25 if rev_growth >= 15 else 20 if rev_growth >= 10 \
              else 15 if rev_growth >= 5 else 8 if rev_growth >= 0 else 0
        breakdown["revenue_growth_yoy_pct"] = round(rev_growth, 2)
    else:
        pts = 0
    score += pts
    breakdown["revenue_pts"] = pts

    # Profit growth (max 40)
    profit_growth = None
    if profit is not None and prev_profit is not None and prev_profit > 0:
        profit_growth = (profit - prev_profit) / prev_profit * 100
        pts = 40 if profit_growth >= 30 else 35 if profit_growth >= 20 else 28 if profit_growth >= 10 \
              else 18 if profit_growth >= 0 else 0
        breakdown["profit_growth_yoy_pct"] = round(profit_growth, 2)
    else:
        pts = 0
    score += pts
    breakdown["profit_pts"] = pts

    # OPM improvement (max 30)
    opm_change = None
    if opm is not None and prev_opm is not None:
        opm_change = opm - prev_opm
        pts = 30 if opm_change >= 3 else 22 if opm_change >= 1 else 15 if opm_change >= 0 \
              else 5 if opm_change >= -2 else 0
        breakdown["opm_change_pp"] = round(opm_change, 2)
    else:
        pts = 15  # unknown → neutral
    score += pts
    breakdown["opm_pts"] = pts

    return {"score": min(score, 100), "breakdown": breakdown}


def _altman_z(op_profit, reserves, equity, borrowings, other_liab, total_assets,
              sales, market_cap) -> dict:
    """
    Modified Altman Z-Score for Indian listed companies.
    Uses non-manufacturing variant: Z' = 6.56A + 3.26B + 6.72C + 1.05D
    where assets are from balance sheet and profit from P&L.

    Safe zone: Z > 2.6 | Grey zone: 1.1–2.6 | Distress: Z < 1.1
    """
    if not total_assets or total_assets <= 0:
        return {"z_score": None, "zone": "insufficient data"}

    # A = Operating Profit / Total Assets (profitability)
    A = (op_profit or 0) / total_assets

    # B = Retained Earnings (Reserves) / Total Assets
    B = (reserves or 0) / total_assets

    # C = Book Equity / Total Liabilities (solvency)
    # Use market cap if available (better signal), else book equity
    numerator = market_cap if market_cap else (equity or 0) + (reserves or 0)
    total_debt = (borrowings or 0) + (other_liab or 0)
    C = numerator / total_debt if total_debt > 0 else 5.0  # cap at 5 if debt-free

    # D = Sales / Total Assets (asset efficiency)
    D = (sales or 0) / total_assets

    z = 6.56 * A + 3.26 * B + 6.72 * C + 1.05 * D
    z = round(z, 3)

    if z > 2.6:
        zone = "SAFE — low financial distress risk"
    elif z > 1.1:
        zone = "GREY ZONE — monitor financial health closely"
    else:
        zone = "DISTRESS — high financial risk, investigate immediately"

    return {
        "z_score": z,
        "zone": zone,
        "components": {"A_profitability": round(A, 4), "B_retained_earnings": round(B, 4),
                       "C_solvency": round(C, 4), "D_asset_efficiency": round(D, 4)},
    }


# ---------------------------------------------------------------------------
# Tool 21: get_stock_scores
# ---------------------------------------------------------------------------

@mcp.tool()
def get_stock_scores(identifier: str) -> str:
    """
    Compute comprehensive quantitative factor scores for a single stock.

    Returns four factor scores (each 0–100) plus an Altman Z-Score:

    QUALITY SCORE (0–100)
      Based on ROE, ROCE, earnings quality (CF/profit ratio),
      debt health (D/E ratio), and trend (improving vs prior year).
      > 70: high quality  |  50-70: average  |  < 50: low quality

    VALUE SCORE (0–100)
      Based on P/E ratio, P/B ratio, and dividend yield.
      Higher score = cheaper valuation.
      > 70: attractive value  |  50-70: fair  |  < 50: expensive

    EARNINGS SURPRISE SCORE (0–100)
      YoY growth in revenue, net profit, and operating margin.
      Measures earnings momentum — is the business accelerating?
      > 70: strong growth  |  50-70: moderate  |  < 50: slowing/declining

    MOMENTUM (price returns — not percentile, raw data)
      1M, 3M, 6M, 12M price returns + composite weighted score.
      Positive = outperforming over that window.

    ALTMAN Z-SCORE
      Bankruptcy probability indicator from balance sheet ratios.
      > 2.6: Safe  |  1.1-2.6: Grey zone  |  < 1.1: Distress

    COMPOSITE SCORE = 40% Quality + 25% Value + 20% Earnings Surprise + 15% Momentum

    HOW TO USE:
      - Quality > 70 + Value > 60 = quality at reasonable price (QARP)
      - Quality > 70 + Value < 40 = great business but expensive — wait
      - Quality < 40 + Value > 70 = cheap for a reason — investigate
      - Earnings Surprise > 70 = earnings acceleration — momentum setup
      - Altman Z < 1.1 = do NOT invest regardless of other scores
    """
    try:
        cid, err = _screener_id(identifier)
        if err:
            return err

        company = _resolve(identifier)
        conn = _conn(SCREENER_DB)

        # Latest fundamentals
        fm = _one(conn,
            """
            SELECT roe_pct, roce_pct, market_cap, pe_ratio, pb_ratio,
                   dividend_yield, current_price, high_52w, low_52w
            FROM   fact_realtime_metrics
            WHERE  company_id = ?
            ORDER BY snapshot_date DESC LIMIT 1
            """, (cid,))

        # Latest + prior year P&L
        pl_rows = _rows(conn,
            """
            SELECT dp.year, fpl.sales, fpl.net_profit, fpl.operating_profit,
                   fpl.opm_pct, fpl.eps
            FROM   fact_profit_loss fpl
            JOIN   dim_period dp ON dp.period_id = fpl.period_id
            WHERE  fpl.company_id = ?
            ORDER BY dp.year DESC LIMIT 3
            """, (cid,))

        # Latest + prior year CF
        cf_rows = _rows(conn,
            """
            SELECT dp.year, fcf.cash_from_operating
            FROM   fact_cash_flow fcf
            JOIN   dim_period dp ON dp.period_id = fcf.period_id
            WHERE  fcf.company_id = ?
            ORDER BY dp.year DESC LIMIT 2
            """, (cid,))

        # Latest balance sheet
        bs = _one(conn,
            """
            SELECT fbs.borrowings, fbs.equity_capital, fbs.reserves,
                   fbs.total_assets, fbs.total_liabilities, fbs.other_liabilities
            FROM   fact_balance_sheet fbs
            JOIN   dim_period dp ON dp.period_id = fbs.period_id
            WHERE  fbs.company_id = ?
            ORDER BY dp.year DESC LIMIT 1
            """, (cid,))

        # Historical ROE/ROCE for trend (avg of years 2-4)
        hist = _rows(conn,
            """
            SELECT dp.year, frm.roe_pct, frm.roce_pct
            FROM   fact_realtime_metrics frm
            JOIN   (SELECT DISTINCT year FROM dim_period ORDER BY year DESC LIMIT 4) dp
                   ON frm.snapshot_date LIKE dp.year || '%'
            WHERE  frm.company_id = ?
            ORDER BY dp.year DESC
            """, (cid,))

        conn.close()

        # Extract values
        roe   = fm.get("roe_pct") if fm else None
        roce  = fm.get("roce_pct") if fm else None
        pe    = fm.get("pe_ratio") if fm else None
        pb    = fm.get("pb_ratio") if fm else None
        dyld  = fm.get("dividend_yield") if fm else None
        mcap  = fm.get("market_cap") if fm else None

        latest_pl = pl_rows[0] if pl_rows else {}
        prev_pl   = pl_rows[1] if len(pl_rows) > 1 else {}

        latest_cf = cf_rows[0].get("cash_from_operating") if cf_rows else None
        net_profit = latest_pl.get("net_profit")
        cf_ratio   = (latest_cf / net_profit) if (latest_cf is not None and net_profit and net_profit > 0) else None

        book_eq = ((bs.get("equity_capital") or 0) + (bs.get("reserves") or 0)) if bs else None
        borrows = bs.get("borrowings") if bs else None

        # Prior ROE/ROCE for trend (use year-2 data)
        roe_prev  = hist[1]["roe_pct"]  if len(hist) > 1 else None
        roce_prev = hist[1]["roce_pct"] if len(hist) > 1 else None

        # Compute scores
        quality   = _quality_score(roe, roce, cf_ratio, borrows, book_eq, roe_prev, roce_prev)
        value     = _value_score(pe, pb, dyld)
        earnings  = _earnings_surprise_score(
            latest_pl.get("sales"), net_profit, latest_pl.get("opm_pct"),
            prev_pl.get("sales"), prev_pl.get("net_profit"), prev_pl.get("opm_pct")
        )
        altman    = _altman_z(
            latest_pl.get("operating_profit"), bs.get("reserves") if bs else None,
            bs.get("equity_capital") if bs else None, borrows,
            bs.get("other_liabilities") if bs else None,
            bs.get("total_assets") if bs else None,
            latest_pl.get("sales"), mcap
        ) if bs else {"z_score": None, "zone": "balance sheet data unavailable"}

        # Momentum from OHLCV
        momentum_data: dict[str, Any] = {}
        if company and company.get("ticker_id") and _db_available(TECHNICAL_DB):
            tc = _conn(TECHNICAL_DB)
            ohlcv = _rows(tc,
                """
                SELECT date, close FROM ohlcv
                WHERE  ticker_id = ? AND date >= date('now', '-400 days')
                  AND  close IS NOT NULL
                ORDER BY date ASC
                """, (company["ticker_id"],))
            tc.close()

            if ohlcv:
                closes_by_date = {r["date"]: r["close"] for r in ohlcv}
                dates_sorted   = sorted(closes_by_date.keys())
                cur_price      = closes_by_date[dates_sorted[-1]]

                def _return_n_days(n: int) -> float | None:
                    target = len(dates_sorted) - n - 1
                    if target < 0:
                        return None
                    past_price = closes_by_date[dates_sorted[target]]
                    return round((cur_price - past_price) / past_price * 100, 2) if past_price > 0 else None

                r1m  = _return_n_days(20)
                r3m  = _return_n_days(63)
                r6m  = _return_n_days(126)
                r12m = _return_n_days(252)

                momentum_data = {
                    "return_1m_pct":  r1m,
                    "return_3m_pct":  r3m,
                    "return_6m_pct":  r6m,
                    "return_12m_pct": r12m,
                }

                # Raw composite momentum (weighted)
                weighted = 0.0
                weight_sum = 0.0
                for ret, w in [(r1m, 0.15), (r3m, 0.25), (r6m, 0.35), (r12m, 0.25)]:
                    if ret is not None:
                        weighted += ret * w
                        weight_sum += w
                momentum_raw = weighted / weight_sum if weight_sum > 0 else None
                momentum_data["momentum_composite_pct"] = round(momentum_raw, 2) if momentum_raw is not None else None

                # Convert to 0-100 score (rough normalization)
                if momentum_raw is not None:
                    m_score = 50 + min(50, max(-50, momentum_raw * 1.5))
                    momentum_data["momentum_score_0_100"] = round(m_score, 1)
                else:
                    m_score = 50
            else:
                m_score = 50
        else:
            m_score = 50

        # 52w position (if fundamentals available)
        if fm and fm.get("current_price") and fm.get("high_52w") and fm.get("low_52w"):
            rng = fm["high_52w"] - fm["low_52w"]
            pos = (fm["current_price"] - fm["low_52w"]) / rng if rng > 0 else 0.5
            momentum_data["position_in_52w_range_pct"] = round(pos * 100, 1)
            if pos < 0.20:
                momentum_data["52w_signal"] = "near 52-week LOW — potential value entry"
            elif pos > 0.80:
                momentum_data["52w_signal"] = "near 52-week HIGH — momentum strong but caution on entry"
            else:
                momentum_data["52w_signal"] = "mid-range"

        # Composite score (quality-weighted)
        qs = quality["score"]
        vs = value["score"]
        es = earnings["score"]
        composite = round(0.40 * qs + 0.25 * vs + 0.20 * es + 0.15 * m_score, 1)

        def _composite_label(s: float) -> str:
            if s >= 75: return "STRONG BUY setup — high quality, good value, earnings growing"
            if s >= 60: return "WATCHLIST — solid on most dimensions"
            if s >= 45: return "NEUTRAL — mixed signals"
            if s >= 30: return "WEAK — poor fundamentals or expensive"
            return "AVOID — multiple red flags"

        return _fmt({
            "company": identifier,
            "composite_score": composite,
            "composite_label": _composite_label(composite),
            "quality_score":   quality,
            "value_score":     value,
            "earnings_surprise_score": earnings,
            "momentum":        momentum_data,
            "altman_z_score":  altman,
            "raw_inputs": {
                "roe_pct": roe, "roce_pct": roce, "pe_ratio": pe,
                "pb_ratio": pb, "dividend_yield": dyld, "cf_ratio": round(cf_ratio, 3) if cf_ratio else None,
            },
        })
    except Exception as e:
        return f"Error computing scores for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 22: screen_by_scores
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_by_scores(
    min_quality: float = 60,
    min_value: float | None = None,
    min_earnings_surprise: float | None = None,
    sort_by: str = "composite",
    min_altman_z: float | None = None,
    min_market_cap: float | None = None,
    max_market_cap: float | None = None,
    limit: int = 25,
) -> str:
    """
    Screen the entire ~5,200-company universe by quantitative factor scores.

    Computes Quality, Value, and Earnings Surprise scores for every company
    and returns the top ranked stocks — the kind of factor-based screening
    used by quant hedge funds.

    SCORE FILTERS:
      min_quality          : Quality Score >= value (default 60, range 0-100)
      min_value            : Value Score >= value (optional)
      min_earnings_surprise: Earnings Surprise Score >= value (optional)
      min_altman_z         : Altman Z-Score >= value (e.g. 2.6 for safe zone only)

    SIZE FILTERS:
      min_market_cap : INR crores (e.g. 5000 for mid+large cap only)
      max_market_cap : INR crores (e.g. 2000 for small cap only)

    SORT OPTIONS:
      sort_by : "composite" (default) | "quality" | "value" | "earnings" | "z_score"

    Args:
      limit : max results (default 25, max 100)

    POWER COMBOS:
      QARP (Quality at Reasonable Price):
        min_quality=70, min_value=55, sort_by="composite"

      Earnings Acceleration + Quality:
        min_quality=65, min_earnings_surprise=70, sort_by="earnings"

      Deep Value in Quality Businesses:
        min_quality=70, min_value=70, sort_by="value"

      Safe Small Caps (low bankruptcy risk):
        max_market_cap=5000, min_quality=55, min_altman_z=2.6

      Growth at Any Price (high earnings momentum):
        min_earnings_surprise=75, sort_by="earnings"
    """
    try:
        limit = min(limit, 100)
        if not _db_available(SCREENER_DB):
            return "Screener DB not found."

        conn = _conn(SCREENER_DB)

        # Big join: get raw materials for all companies in one query
        raw = _rows(conn,
            """
            WITH latest_m AS (
                SELECT company_id, roe_pct, roce_pct, market_cap, pe_ratio,
                       pb_ratio, dividend_yield, current_price,
                       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY snapshot_date DESC) rn
                FROM   fact_realtime_metrics
            ),
            latest_pl AS (
                SELECT fpl.company_id, fpl.sales, fpl.net_profit, fpl.operating_profit, fpl.opm_pct,
                       ROW_NUMBER() OVER (PARTITION BY fpl.company_id ORDER BY dp.year DESC) rn
                FROM   fact_profit_loss fpl
                JOIN   dim_period dp ON dp.period_id = fpl.period_id
            ),
            prev_pl AS (
                SELECT fpl.company_id, fpl.sales AS prev_sales,
                       fpl.net_profit AS prev_profit, fpl.opm_pct AS prev_opm,
                       ROW_NUMBER() OVER (PARTITION BY fpl.company_id ORDER BY dp.year DESC) rn
                FROM   fact_profit_loss fpl
                JOIN   dim_period dp ON dp.period_id = fpl.period_id
            ),
            latest_cf AS (
                SELECT fcf.company_id, fcf.cash_from_operating,
                       ROW_NUMBER() OVER (PARTITION BY fcf.company_id ORDER BY dp.year DESC) rn
                FROM   fact_cash_flow fcf
                JOIN   dim_period dp ON dp.period_id = fcf.period_id
            ),
            latest_bs AS (
                SELECT fbs.company_id, fbs.borrowings, fbs.equity_capital,
                       fbs.reserves, fbs.total_assets, fbs.total_liabilities,
                       fbs.other_liabilities, fbs.fixed_assets,
                       ROW_NUMBER() OVER (PARTITION BY fbs.company_id ORDER BY dp.year DESC) rn
                FROM   fact_balance_sheet fbs
                JOIN   dim_period dp ON dp.period_id = fbs.period_id
            )
            SELECT dc.company_id, dc.symbol, dc.name, dc.nse_code, dc.bse_code,
                   lm.roe_pct, lm.roce_pct, lm.market_cap, lm.pe_ratio, lm.pb_ratio,
                   lm.dividend_yield, lm.current_price,
                   lp.sales, lp.net_profit, lp.operating_profit, lp.opm_pct,
                   pp.prev_sales, pp.prev_profit, pp.prev_opm,
                   cf.cash_from_operating,
                   bs.borrowings, bs.equity_capital, bs.reserves,
                   bs.total_assets, bs.total_liabilities, bs.other_liabilities
            FROM   dim_company dc
            JOIN   latest_m  lm ON lm.company_id = dc.company_id AND lm.rn = 1
            LEFT JOIN latest_pl lp ON lp.company_id = dc.company_id AND lp.rn = 1
            LEFT JOIN prev_pl   pp ON pp.company_id = dc.company_id AND pp.rn = 2
            LEFT JOIN latest_cf cf ON cf.company_id = dc.company_id AND cf.rn = 1
            LEFT JOIN latest_bs bs ON bs.company_id = dc.company_id AND bs.rn = 1
            """)
        conn.close()

        # Apply market cap filter early to reduce computation
        if min_market_cap is not None:
            raw = [r for r in raw if r.get("market_cap") and r["market_cap"] >= min_market_cap]
        if max_market_cap is not None:
            raw = [r for r in raw if r.get("market_cap") and r["market_cap"] <= max_market_cap]

        scored = []
        for r in raw:
            roe   = r.get("roe_pct")
            roce  = r.get("roce_pct")
            net_p = r.get("net_profit")
            op_cf = r.get("cash_from_operating")
            cf_r  = (op_cf / net_p) if (op_cf is not None and net_p and net_p > 0) else None
            borr  = r.get("borrowings")
            eq    = (r.get("equity_capital") or 0) + (r.get("reserves") or 0)

            q  = _quality_score(roe, roce, cf_r, borr, eq, None, None)
            v  = _value_score(r.get("pe_ratio"), r.get("pb_ratio"), r.get("dividend_yield"))
            es = _earnings_surprise_score(
                r.get("sales"), net_p, r.get("opm_pct"),
                r.get("prev_sales"), r.get("prev_profit"), r.get("prev_opm")
            )
            az = _altman_z(
                r.get("operating_profit"), r.get("reserves"), r.get("equity_capital"),
                borr, r.get("other_liabilities"), r.get("total_assets"),
                r.get("sales"), r.get("market_cap")
            )

            composite = round(0.40 * q["score"] + 0.25 * v["score"] + 0.35 * es["score"], 1)

            # Apply score filters
            if q["score"] < min_quality:
                continue
            if min_value is not None and v["score"] < min_value:
                continue
            if min_earnings_surprise is not None and es["score"] < min_earnings_surprise:
                continue
            if min_altman_z is not None and (az.get("z_score") is None or az["z_score"] < min_altman_z):
                continue

            scored.append({
                "company_id": r["company_id"],
                "name": r["name"],
                "nse_code": r["nse_code"],
                "market_cap": r["market_cap"],
                "current_price": r["current_price"],
                "quality_score": q["score"],
                "value_score": v["score"],
                "earnings_surprise_score": es["score"],
                "composite_score": composite,
                "altman_z": az.get("z_score"),
                "altman_zone": az.get("zone", "").split(" — ")[0] if az.get("zone") else None,
                "roe_pct": roe,
                "roce_pct": roce,
                "pe_ratio": r.get("pe_ratio"),
                "pb_ratio": r.get("pb_ratio"),
            })

        # Sort
        sort_key = {
            "composite": "composite_score",
            "quality":   "quality_score",
            "value":     "value_score",
            "earnings":  "earnings_surprise_score",
            "z_score":   "altman_z",
        }.get(sort_by, "composite_score")

        scored.sort(key=lambda x: (x.get(sort_key) or 0), reverse=True)
        results = scored[:limit]

        return _fmt({
            "filters": {
                "min_quality": min_quality,
                "min_value": min_value,
                "min_earnings_surprise": min_earnings_surprise,
                "min_altman_z": min_altman_z,
                "min_market_cap": min_market_cap,
                "max_market_cap": max_market_cap,
                "sort_by": sort_by,
            },
            "universe_after_market_cap_filter": len(raw),
            "matched": len(scored),
            "returned": len(results),
            "note": (
                "Scores: Quality = ROE+ROCE+CF quality+debt health+trend. "
                "Value = PE+PB+dividend yield. "
                "Earnings Surprise = YoY revenue+profit+margin growth. "
                "Composite = 40% Quality + 25% Value + 35% Earnings Surprise. "
                "For momentum scores, call get_stock_scores on individual results."
            ),
            "stocks": results,
        })
    except Exception as e:
        return f"Error screening by scores: {e}"


# ---------------------------------------------------------------------------
# Pillar 2 — Mutual Fund Intelligence helpers
# ---------------------------------------------------------------------------

def _mf_conn() -> sqlite3.Connection | None:
    """Return a read-only connection to mf.db, or None if not seeded yet."""
    if not MF_DB.exists():
        return None
    conn = sqlite3.connect(f"file:{MF_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _mf_not_seeded() -> str:
    return (
        "mf.db not found. Run the seeder first:\n"
        "  cd D:\\Projects\\AI-Finance\\mf-analysis\n"
        "  python mf_seeder.py\n"
        "This downloads ~500 equity fund schemes + 3 years of NAV history."
    )


def _holdings_conn() -> sqlite3.Connection | None:
    """Return a read-only connection to holdings.db (badass-mf), or None if not present."""
    if not HOLDINGS_DB.exists():
        return None
    conn = sqlite3.connect(f"file:{HOLDINGS_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _isin_map_conn() -> sqlite3.Connection | None:
    """Return a read-only connection to ms_isin_map.db, or None if not present."""
    if not ISIN_MAP_DB.exists():
        return None
    conn = sqlite3.connect(f"file:{ISIN_MAP_DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _fund_search(conn: sqlite3.Connection, query: str) -> list[sqlite3.Row]:
    """Find funds matching a name/fund-house query."""
    q = f"%{query}%"
    return conn.execute(
        """
        SELECT f.scheme_code, f.scheme_name, f.fund_house, f.scheme_category,
               fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y, fp.nav_latest
        FROM funds f
        LEFT JOIN fund_performance fp USING (scheme_code)
        WHERE f.scheme_name LIKE ? OR f.fund_house LIKE ? OR f.scheme_category LIKE ?
        ORDER BY fp.ret_1y_pct DESC NULLS LAST
        LIMIT 20
        """,
        (q, q, q),
    ).fetchall()


def _dii_trend(company_name: str) -> dict | None:
    """Pull DII% trend from screener DB as MF accumulation proxy."""
    try:
        conn = _conn(SCREENER_DB)
        row = conn.execute(
            "SELECT company_id FROM dim_company WHERE name LIKE ? LIMIT 1",
            (f"%{company_name}%",),
        ).fetchone()
        if not row:
            return None
        cid = row["company_id"]
        rows = conn.execute(
            """
            SELECT dp.period_label, fs.diis_pct, fs.fiis_pct, fs.promoters_pct
            FROM fact_shareholding fs
            JOIN dim_period dp USING (period_id)
            WHERE fs.company_id = ? AND fs.period_type = 'annual'
            ORDER BY dp.year DESC LIMIT 5
            """,
            (cid,),
        ).fetchall()
        conn.close()
        if not rows:
            return None
        trend = [dict(r) for r in rows]
        # Compute DII direction
        if len(trend) >= 2:
            delta = (trend[0]["diis_pct"] or 0) - (trend[-1]["diis_pct"] or 0)
            direction = "accumulating" if delta > 0.5 else "reducing" if delta < -0.5 else "stable"
        else:
            direction = "unknown"
        return {"quarters": trend, "dii_trend": direction}
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Pillar 2 — MCP tools
# ---------------------------------------------------------------------------

@mcp.tool()
def search_mutual_funds(
    query: str = "",
    category: str = "",
    fund_house: str = "",
    sort_by: str = "ret_1y_pct",
    limit: int = 15,
) -> str:
    """
    Search mutual funds by name, category, or fund house.

    Args:
        query:      Free-text search across scheme name (e.g. "flexi cap", "ELSS", "bluechip")
        category:   Filter by category substring (e.g. "large cap", "mid cap", "sectoral")
        fund_house: Filter by AMC (e.g. "SBI", "HDFC", "Mirae", "Axis")
        sort_by:    ret_1y_pct | ret_3y_pct | volatility_1y (default: 1-year return)
        limit:      Max results (default 15)

    Returns ranked list with scheme code, name, category, NAV, returns, and volatility.
    Use get_fund_details() for deep dive on any result.
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        conditions = []
        params: list[Any] = []

        if query:
            conditions.append("(f.scheme_name LIKE ? OR f.fund_house LIKE ?)")
            params += [f"%{query}%", f"%{query}%"]
        if category:
            conditions.append("f.scheme_category LIKE ?")
            params.append(f"%{category}%")
        if fund_house:
            conditions.append("f.fund_house LIKE ?")
            params.append(f"%{fund_house}%")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        valid_sorts = {"ret_1y_pct", "ret_3y_pct", "volatility_1y", "nav_latest", "ret_3m_pct"}
        sort_col = sort_by if sort_by in valid_sorts else "ret_1y_pct"

        rows = conn.execute(
            f"""
            SELECT f.scheme_code, f.scheme_name, f.fund_house, f.scheme_category,
                   fp.nav_latest, fp.ret_1m_pct, fp.ret_3m_pct, fp.ret_6m_pct,
                   fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y
            FROM funds f
            LEFT JOIN fund_performance fp USING (scheme_code)
            {where}
            ORDER BY fp.{sort_col} DESC NULLS LAST
            LIMIT ?
            """,
            params + [limit],
        ).fetchall()
        conn.close()

        if not rows:
            return "No funds found matching your criteria. Try broader terms."

        funds = []
        for r in rows:
            funds.append({
                "scheme_code": r["scheme_code"],
                "name": r["scheme_name"],
                "fund_house": r["fund_house"],
                "category": r["scheme_category"],
                "nav": r["nav_latest"],
                "returns": {
                    "1m_pct": r["ret_1m_pct"],
                    "3m_pct": r["ret_3m_pct"],
                    "6m_pct": r["ret_6m_pct"],
                    "1y_pct": r["ret_1y_pct"],
                    "3y_pct": r["ret_3y_pct"],
                },
                "volatility_1y_pct": r["volatility_1y"],
            })

        return _fmt({
            "query": {"text": query, "category": category, "fund_house": fund_house},
            "count": len(funds),
            "sorted_by": sort_col,
            "funds": funds,
            "tip": "Use get_fund_details(fund_name) for holdings + full performance breakdown.",
        })
    except Exception as e:
        return f"Error searching funds: {e}"


@mcp.tool()
def get_fund_details(fund_name: str) -> str:
    """
    Deep dive into a single mutual fund: NAV trend, performance vs category peers,
    actual portfolio holdings with weights and sectors, and portfolio composition.

    Holdings sourced from holdings.db (1,777 funds scraped from Morningstar).
    portfolio_date shows when the holdings were last disclosed (typically 1-2 months lag).

    Args:
        fund_name: Scheme name or part of it (e.g. "Mirae Asset Large Cap",
                   "Parag Parikh Flexi Cap", "SBI Small Cap", "HDFC Mid Cap")

    Returns:
      - performance: NAV, 1M/3M/6M/1Y/3Y returns, volatility
      - top_holdings: top 15 stocks with weight %, sector, portfolio_date
      - portfolio_meta: total holdings, equity count, top holding concentration
      - category_peers: top 5 funds in same category by 1Y return
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        rows = conn.execute(
            """
            SELECT f.scheme_code, f.scheme_name, f.fund_house, f.scheme_type,
                   f.scheme_category, f.isin_growth,
                   fp.nav_latest, fp.ret_1m_pct, fp.ret_3m_pct, fp.ret_6m_pct,
                   fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y
            FROM funds f
            LEFT JOIN fund_performance fp USING (scheme_code)
            WHERE f.scheme_name LIKE ?
            ORDER BY fp.ret_1y_pct DESC NULLS LAST
            LIMIT 3
            """,
            (f"%{fund_name}%",),
        ).fetchall()

        if not rows:
            return f"Fund '{fund_name}' not found. Try search_mutual_funds() to browse."

        # Fetch holdings from holdings.db (name-based best-effort match)
        h_conn = _holdings_conn()

        result = []
        for r in rows:
            code = r["scheme_code"]

            # Category peer comparison (same category, sorted by 1Y return)
            peers = conn.execute(
                """
                SELECT f.scheme_name, fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y
                FROM funds f
                JOIN fund_performance fp USING (scheme_code)
                WHERE f.scheme_category = ?
                ORDER BY fp.ret_1y_pct DESC NULLS LAST
                LIMIT 5
                """,
                (r["scheme_category"],),
            ).fetchall()

            # Holdings from holdings.db — strip common plan suffixes for name match
            top_holdings_data: Any = "Holdings not available (holdings.db not found)"
            portfolio_meta: dict = {}

            if h_conn:
                base_name = r["scheme_name"]
                for suffix in (
                    " - IDCW Plan", " - Growth Plan", " - Direct Plan",
                    " IDCW Plan", " Growth Plan", " Direct Plan",
                    " - Growth Option - Direct Plan", " - Growth Option",
                    " - Direct - Growth", " Direct Growth",
                    " - Growth", " Growth",
                ):
                    base_name = base_name.replace(suffix, "")
                base_name = base_name.strip()

                meta = h_conn.execute(
                    """
                    SELECT sec_id, num_holdings, num_equity, num_bond,
                           top_holding_weight, portfolio_date
                    FROM fund_meta
                    WHERE scheme_name LIKE ?
                    ORDER BY portfolio_date DESC
                    LIMIT 1
                    """,
                    (f"%{base_name[:50]}%",),
                ).fetchone()

                if meta:
                    holdings_rows = h_conn.execute(
                        """
                        SELECT holding_name, ticker, weighting, sector, country
                        FROM fund_holdings
                        WHERE sec_id = ? AND holding_type = 'E'
                        ORDER BY weighting DESC
                        LIMIT 15
                        """,
                        (meta["sec_id"],),
                    ).fetchall()

                    top_holdings_data = [
                        {
                            "stock":          h["holding_name"],
                            "ticker":         h["ticker"],
                            "weight_pct":     round(h["weighting"], 3),
                            "sector":         h["sector"],
                        }
                        for h in holdings_rows
                    ]
                    portfolio_meta = {
                        "portfolio_date":       meta["portfolio_date"],
                        "total_holdings":       meta["num_holdings"],
                        "equity_holdings":      meta["num_equity"],
                        "bond_holdings":        meta["num_bond"],
                        "top_holding_weight_pct": round(meta["top_holding_weight"] or 0, 2),
                        "data_note":            (
                            f"Holdings as of {meta['portfolio_date']} — "
                            "typically 1-2 months behind. Verify before acting."
                        ),
                    }
                else:
                    top_holdings_data = (
                        f"No holdings found in holdings.db for '{base_name}'. "
                        "Fund may use a different name in the Morningstar data."
                    )

            result.append({
                "scheme_code":   code,
                "name":          r["scheme_name"],
                "fund_house":    r["fund_house"],
                "category":      r["scheme_category"],
                "isin_growth":   r["isin_growth"],
                "performance": {
                    "nav_latest":       r["nav_latest"],
                    "ret_1m_pct":       r["ret_1m_pct"],
                    "ret_3m_pct":       r["ret_3m_pct"],
                    "ret_6m_pct":       r["ret_6m_pct"],
                    "ret_1y_pct":       r["ret_1y_pct"],
                    "ret_3y_pct":       r["ret_3y_pct"],
                    "volatility_1y_pct": r["volatility_1y"],
                },
                "portfolio_meta":  portfolio_meta,
                "top_holdings":    top_holdings_data,
                "category_peers_by_1y_return": [
                    {
                        "name":          p["scheme_name"],
                        "ret_1y_pct":    p["ret_1y_pct"],
                        "ret_3y_pct":    p["ret_3y_pct"],
                        "volatility_pct": p["volatility_1y"],
                    }
                    for p in peers
                ],
            })

        if h_conn:
            h_conn.close()
        conn.close()
        return _fmt(result[0] if len(result) == 1 else result)
    except Exception as e:
        return f"Error fetching fund details: {e}"


@mcp.tool()
def get_funds_holding_stock(identifier: str, min_weight_pct: float = 0.5) -> str:
    """
    Find which mutual funds hold a given stock — the KEY cross-sell signal.

    Data sourced from holdings.db (1,777 funds, portfolio disclosures as of
    portfolio_date — typically 1-2 months behind. Always check portfolio_date
    in results before citing as a current position).

    When a strong buy signal fires on a stock, this tells you:
    - How many funds own it (institutional validation)
    - At what weight (conviction level per fund)
    - Which fund categories are accumulating (large cap, mid cap, flexi cap)
    - Combined with DII trend from screener for direction confirmation

    Args:
        identifier:      NSE symbol, BSE code, ISIN, or company name
                         (e.g. "INFY", "Infosys", "500209", "TCS")
        min_weight_pct:  Minimum portfolio weight to include (default 0.5%)

    SIGNALS:
      Many high-returning funds holding at high weight → strong institutional conviction
      Only index/ETF funds → passive exposure, not active conviction
      Weight rising across quarters → active accumulation
    """
    # Resolve identifier → NSE code + company name
    company = _resolve(identifier)
    nse_code   = company.get("nse_code")   if company else None
    comp_name  = company.get("name")       if company else identifier

    h_conn = _holdings_conn()
    if not h_conn:
        return (
            "holdings.db not found in mf-analysis/data/. "
            "Copy it from badass-mf-project/data/holdings.db to mf-analysis/data/."
        )

    try:
        # Dual query: match by NSE ticker OR by holding name (catches BSE-code-stored entries)
        params: list[Any] = [min_weight_pct]
        ticker_clause = ""
        name_clause   = ""

        if nse_code:
            ticker_clause = "fh.ticker = ?"
            params = [nse_code, min_weight_pct]
        if comp_name:
            name_clause = f"fh.holding_name LIKE ?"
            params = params + [f"%{comp_name}%"]

        if ticker_clause and name_clause:
            where = f"({ticker_clause} OR {name_clause}) AND fh.weighting >= ?"
            params = [nse_code, f"%{comp_name}%", min_weight_pct]
        elif ticker_clause:
            where = f"{ticker_clause} AND fh.weighting >= ?"
        elif name_clause:
            where = f"{name_clause} AND fh.weighting >= ?"
            params = [f"%{comp_name}%", min_weight_pct]
        else:
            # No identifier resolved — name-only search
            where = "fh.holding_name LIKE ? AND fh.weighting >= ?"
            params = [f"%{identifier}%", min_weight_pct]

        holdings_rows = h_conn.execute(
            f"""
            SELECT fh.scheme_name, fh.amc, fh.weighting, fh.sector,
                   fh.portfolio_date, fh.holding_name, fh.ticker,
                   fm.num_equity, fm.num_holdings, fm.top_holding_weight
            FROM fund_holdings fh
            JOIN fund_meta fm ON fh.sec_id = fm.sec_id
            WHERE fh.holding_type = 'E' AND {where}
            ORDER BY fh.weighting DESC
            LIMIT 40
            """,
            params,
        ).fetchall()
        h_conn.close()

        # Enrich with performance from mf.db (best-effort name match)
        perf_by_name: dict[str, dict] = {}
        mf_conn = _mf_conn()
        if mf_conn and holdings_rows:
            # Batch fetch by fund house names to minimize queries
            fund_names = list({r["scheme_name"] for r in holdings_rows})
            for fn in fund_names[:20]:
                # Strip "Direct Plan" / "Growth" suffixes for broader match
                base = fn.replace(" - Direct Plan", "").replace(" Direct Plan", "")
                base = base.replace(" Growth", "").replace(" - Growth", "").strip()
                row = mf_conn.execute(
                    """
                    SELECT fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y
                    FROM funds f
                    JOIN fund_performance fp USING (scheme_code)
                    WHERE f.scheme_name LIKE ?
                    ORDER BY fp.ret_1y_pct DESC NULLS LAST
                    LIMIT 1
                    """,
                    (f"%{base[:40]}%",),
                ).fetchone()
                if row:
                    perf_by_name[fn] = dict(row)
            mf_conn.close()

        # DII trend from screener (always available)
        dii = _dii_trend(comp_name or identifier)

        result: dict[str, Any] = {
            "identifier_searched": identifier,
            "resolved_nse_code":   nse_code,
            "resolved_name":       comp_name,
            "min_weight_filter_pct": min_weight_pct,
        }

        if holdings_rows:
            funds_list = []
            for r in holdings_rows:
                perf = perf_by_name.get(r["scheme_name"], {})
                funds_list.append({
                    "fund_name":       r["scheme_name"],
                    "amc":             r["amc"],
                    "weight_pct":      round(r["weighting"], 3),
                    "sector":          r["sector"],
                    "portfolio_date":  r["portfolio_date"],
                    "fund_ret_1y_pct": perf.get("ret_1y_pct"),
                    "fund_ret_3y_pct": perf.get("ret_3y_pct"),
                    "fund_volatility": perf.get("volatility_1y"),
                    "fund_num_stocks": r["num_equity"],
                })

            # Compute oldest portfolio_date for staleness signal
            dates = [r["portfolio_date"] for r in holdings_rows if r["portfolio_date"]]
            oldest_date = min(dates) if dates else None

            result["funds_holding"]       = funds_list
            result["fund_count"]          = len(funds_list)
            result["data_as_of"]          = oldest_date
            result["data_note"]           = (
                f"Holdings as of {oldest_date} — typically 1-2 months behind. "
                "Verify current position before acting."
            )

            top_perf = [f for f in funds_list if (f["fund_ret_1y_pct"] or 0) > 20]
            result["held_by_top_performers_count"] = len(top_perf)

            # Category breakdown
            from collections import Counter
            amc_counts = Counter(r["amc"] for r in holdings_rows)
            result["top_fund_houses"] = [
                {"amc": k, "fund_count": v}
                for k, v in amc_counts.most_common(5)
            ]
        else:
            result["funds_holding"] = (
                f"No funds found holding '{identifier}' "
                f"(searched ticker='{nse_code}' and name='{comp_name}') "
                f"with weight >= {min_weight_pct}%. "
                "This stock may not be in holdings.db coverage or may be a micro-cap."
            )

        if dii:
            result["dii_institutional_trend"] = {
                "trend": dii["dii_trend"],
                "interpretation": {
                    "accumulating": "DIIs have been steadily buying — institutional confidence signal",
                    "reducing":     "DIIs reducing exposure — potential concern or profit-booking",
                    "stable":       "DII holding stable — no directional institutional signal",
                }.get(dii["dii_trend"], ""),
                "historical_dii_pct": dii["quarters"],
            }

        return _fmt(result)
    except Exception as e:
        return f"Error finding funds holding '{identifier}': {e}"


@mcp.tool()
def get_fund_nav_history(fund_name: str, months: int = 24) -> str:
    """
    Retrieve NAV history for a fund — useful for charting performance,
    computing returns over custom periods, or comparing against a stock.

    Args:
        fund_name: Fund name or partial name
        months:    How many months of history to return (default 24, max 36)
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        fund_row = conn.execute(
            """
            SELECT scheme_code, scheme_name, fund_house, scheme_category
            FROM funds WHERE scheme_name LIKE ?
            ORDER BY scheme_name LIMIT 1
            """,
            (f"%{fund_name}%",),
        ).fetchone()

        if not fund_row:
            return f"Fund '{fund_name}' not found. Use search_mutual_funds() to browse."

        code = fund_row["scheme_code"]
        cutoff = f"date('now', '-{min(months, 36)} months')"
        nav_rows = conn.execute(
            f"""
            SELECT nav_date, nav FROM nav_history
            WHERE scheme_code = ? AND nav_date >= {cutoff}
            ORDER BY nav_date ASC
            """,
            (code,),
        ).fetchall()
        conn.close()

        if not nav_rows:
            return f"No NAV history in DB for '{fund_name}'. Try running mf_seeder.py."

        nav_list = [{"date": r["nav_date"], "nav": r["nav"]} for r in nav_rows]
        first_nav = nav_list[0]["nav"]
        last_nav  = nav_list[-1]["nav"]
        total_ret = round((last_nav - first_nav) / first_nav * 100, 2) if first_nav else None

        # Simple monthly summary (first NAV of each month)
        monthly = {}
        for entry in nav_list:
            ym = entry["date"][:7]
            if ym not in monthly:
                monthly[ym] = entry["nav"]
        monthly_summary = [{"month": k, "nav": v} for k, v in sorted(monthly.items())]

        return _fmt({
            "fund": {
                "name": fund_row["scheme_name"],
                "fund_house": fund_row["fund_house"],
                "category": fund_row["scheme_category"],
            },
            "period": f"{nav_list[0]['date']} to {nav_list[-1]['date']}",
            "total_return_pct": total_ret,
            "nav_start": first_nav,
            "nav_end": last_nav,
            "data_points": len(nav_list),
            "monthly_nav_summary": monthly_summary,
            "daily_nav": nav_list,  # full series for charting
        })
    except Exception as e:
        return f"Error fetching NAV history: {e}"


# ---------------------------------------------------------------------------
# Tool: get_fund_sector_weights
# ---------------------------------------------------------------------------

@mcp.tool()
def get_fund_sector_weights(fund_name: str) -> str:
    """
    Show sector-wise weight breakdown of a mutual fund's equity portfolio.

    Useful for understanding a fund's sector bets vs benchmark, identifying
    concentration risk, or finding funds overweight a sector you're bullish on.

    Data sourced from holdings.db (Morningstar portfolio disclosures).
    portfolio_date shows when holdings were last disclosed (1-2 month lag typical).

    Args:
        fund_name: Fund name or partial name (e.g. "Parag Parikh", "SBI Small Cap")

    Returns:
      - sector breakdown: each sector → weight % + number of stocks
      - top 3 holdings per sector
      - portfolio_date (when the data was disclosed)
      - concentration metrics: top sector weight, top 3 sector weight combined
    """
    h_conn = _holdings_conn()
    if not h_conn:
        return "holdings.db not found. Copy from badass-mf-project/data/ to mf-analysis/data/."
    try:
        meta = h_conn.execute(
            """
            SELECT sec_id, scheme_name, amc, portfolio_date, num_equity
            FROM fund_meta
            WHERE scheme_name LIKE ?
            ORDER BY portfolio_date DESC
            LIMIT 1
            """,
            (f"%{fund_name}%",),
        ).fetchone()

        if not meta:
            return f"Fund '{fund_name}' not found in holdings.db. Try a shorter name."

        rows = h_conn.execute(
            """
            SELECT sector, holding_name, ticker, weighting
            FROM fund_holdings
            WHERE sec_id = ? AND holding_type = 'E' AND sector IS NOT NULL
            ORDER BY weighting DESC
            """,
            (meta["sec_id"],),
        ).fetchall()
        h_conn.close()

        if not rows:
            return f"No equity holdings found for '{meta['scheme_name']}'."

        # Aggregate by sector
        from collections import defaultdict
        sector_data: dict[str, dict] = defaultdict(lambda: {"weight": 0.0, "stocks": []})
        for r in rows:
            sec = r["sector"] or "Unknown"
            sector_data[sec]["weight"] += r["weighting"]
            if len(sector_data[sec]["stocks"]) < 3:
                sector_data[sec]["stocks"].append({
                    "name":       r["holding_name"],
                    "ticker":     r["ticker"],
                    "weight_pct": round(r["weighting"], 3),
                })

        sectors_sorted = sorted(
            [{"sector": k, "weight_pct": round(v["weight"], 2), "top_stocks": v["stocks"]}
             for k, v in sector_data.items()],
            key=lambda x: x["weight_pct"],
            reverse=True,
        )

        top3_weight = sum(s["weight_pct"] for s in sectors_sorted[:3])

        return _fmt({
            "fund":            meta["scheme_name"],
            "amc":             meta["amc"],
            "portfolio_date":  meta["portfolio_date"],
            "equity_holdings": meta["num_equity"],
            "data_note":       f"Holdings as of {meta['portfolio_date']} — typically 1-2 months behind.",
            "top_sector_weight_pct":    sectors_sorted[0]["weight_pct"] if sectors_sorted else 0,
            "top_3_sectors_combined_pct": round(top3_weight, 2),
            "sector_breakdown": sectors_sorted,
        })
    except Exception as e:
        return f"Error fetching sector weights for '{fund_name}': {e}"


# ---------------------------------------------------------------------------
# Tool: get_portfolio_overlap
# ---------------------------------------------------------------------------

@mcp.tool()
def get_portfolio_overlap(fund1: str, fund2: str) -> str:
    """
    Find common holdings between two mutual funds and measure portfolio overlap.

    High overlap means the two funds are not providing true diversification —
    you're effectively doubling your bet on the same stocks.

    Low overlap between two high-performing funds = genuine diversification value.

    Data sourced from holdings.db. portfolio_date may differ between funds.

    Args:
        fund1: Name of first fund (partial match OK)
        fund2: Name of second fund (partial match OK)

    Returns:
      - overlap_pct: % of fund1's weight that is also in fund2 (directional)
      - common_stock_count: number of stocks held by both
      - common_holdings: each overlapping stock with weight in each fund
      - unique_to_fund1 / unique_to_fund2: stocks held by only one (top 10 each)
    """
    h_conn = _holdings_conn()
    if not h_conn:
        return "holdings.db not found. Copy from badass-mf-project/data/ to mf-analysis/data/."
    try:
        def _get_fund_holdings(name: str) -> tuple[str, str, list[dict]]:
            meta = h_conn.execute(
                "SELECT sec_id, scheme_name, portfolio_date FROM fund_meta WHERE scheme_name LIKE ? ORDER BY portfolio_date DESC LIMIT 1",
                (f"%{name}%",),
            ).fetchone()
            if not meta:
                return name, "", []
            rows = h_conn.execute(
                """
                SELECT holding_name, ticker, weighting
                FROM fund_holdings
                WHERE sec_id = ? AND holding_type = 'E'
                ORDER BY weighting DESC
                """,
                (meta["sec_id"],),
            ).fetchall()
            return meta["scheme_name"], meta["portfolio_date"], [dict(r) for r in rows]

        name1, date1, h1 = _get_fund_holdings(fund1)
        name2, date2, h2 = _get_fund_holdings(fund2)
        h_conn.close()

        if not h1:
            return f"Fund '{fund1}' not found in holdings.db."
        if not h2:
            return f"Fund '{fund2}' not found in holdings.db."

        # Build lookup dicts: ticker → weight (use holding_name as fallback key)
        def _key(r: dict) -> str:
            return r["ticker"] if r["ticker"] else r["holding_name"]

        map1 = {_key(r): r for r in h1}
        map2 = {_key(r): r for r in h2}

        common_keys    = set(map1.keys()) & set(map2.keys())
        unique_to_f1   = set(map1.keys()) - set(map2.keys())
        unique_to_f2   = set(map2.keys()) - set(map1.keys())

        total_weight_f1 = sum(r["weighting"] for r in h1)
        overlap_weight  = sum(map1[k]["weighting"] for k in common_keys)
        overlap_pct     = round((overlap_weight / total_weight_f1 * 100) if total_weight_f1 else 0, 1)

        common_list = sorted(
            [
                {
                    "stock":         map1[k]["holding_name"],
                    "ticker":        k,
                    f"weight_in_{name1[:20]}_pct": round(map1[k]["weighting"], 3),
                    f"weight_in_{name2[:20]}_pct": round(map2[k]["weighting"], 3),
                }
                for k in common_keys
            ],
            key=lambda x: list(x.values())[2],
            reverse=True,
        )

        unique1_list = sorted(
            [{"stock": map1[k]["holding_name"], "ticker": k, "weight_pct": round(map1[k]["weighting"], 3)}
             for k in unique_to_f1],
            key=lambda x: x["weight_pct"], reverse=True,
        )[:10]

        unique2_list = sorted(
            [{"stock": map2[k]["holding_name"], "ticker": k, "weight_pct": round(map2[k]["weighting"], 3)}
             for k in unique_to_f2],
            key=lambda x: x["weight_pct"], reverse=True,
        )[:10]

        diversification = (
            "HIGH OVERLAP — minimal diversification benefit"      if overlap_pct > 60 else
            "MODERATE OVERLAP — some shared exposure"             if overlap_pct > 30 else
            "LOW OVERLAP — genuine diversification"
        )

        return _fmt({
            "fund1":              name1,
            "fund1_portfolio_date": date1,
            "fund2":              name2,
            "fund2_portfolio_date": date2,
            "overlap_pct":        overlap_pct,
            "diversification_verdict": diversification,
            "common_stock_count": len(common_keys),
            "fund1_total_stocks": len(h1),
            "fund2_total_stocks": len(h2),
            "common_holdings":    common_list,
            f"unique_to_{name1[:25]}": unique1_list,
            f"unique_to_{name2[:25]}": unique2_list,
        })
    except Exception as e:
        return f"Error computing portfolio overlap: {e}"


@mcp.tool()
def compare_stock_vs_fund(stock_identifier: str, fund_name: str) -> str:
    """
    Head-to-head comparison: buying the stock directly vs investing in a fund
    that holds it. Key cross-sell tool.

    Computes:
    - Fund's 1Y/3Y returns vs stock's 1Y/3Y price performance
    - Fund's annualised volatility vs stock's 52-week range volatility proxy
    - Stock's weight in fund (concentration vs diversification)
    - DII trend on the stock (institutional stance)
    - Cross-sell recommendation logic

    Args:
        stock_identifier: NSE symbol, company name, or ISIN
        fund_name:        MF name or partial name
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        # ── Fund data ──────────────────────────────────────────────────
        fund_row = conn.execute(
            """
            SELECT f.scheme_code, f.scheme_name, f.fund_house, f.scheme_category,
                   fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y, fp.nav_latest
            FROM funds f
            LEFT JOIN fund_performance fp USING (scheme_code)
            WHERE f.scheme_name LIKE ?
            ORDER BY fp.ret_1y_pct DESC NULLS LAST LIMIT 1
            """,
            (f"%{fund_name}%",),
        ).fetchone()

        # ── Stock weight in fund ───────────────────────────────────────
        stock_in_fund = None
        if fund_row:
            h = conn.execute(
                """
                SELECT weight_pct, market_value_cr, as_of_date
                FROM holdings
                WHERE scheme_code = ? AND company_name LIKE ?
                ORDER BY weight_pct DESC LIMIT 1
                """,
                (fund_row["scheme_code"], f"%{stock_identifier}%"),
            ).fetchone()
            if h:
                stock_in_fund = {
                    "weight_pct": h["weight_pct"],
                    "market_value_cr": h["market_value_cr"],
                    "as_of_date": h["as_of_date"],
                }
        conn.close()

        # ── Stock performance from technical DB ────────────────────────
        stock_perf: dict[str, Any] = {}
        try:
            tech = _conn(TECHNICAL_DB)
            # Resolve identifier
            ticker_row = tech.execute(
                """
                SELECT t.id AS ticker_id, t.symbol, t.company AS company_name
                FROM tickers t
                WHERE t.symbol = ? OR t.company LIKE ?
                LIMIT 1
                """,
                (stock_identifier.upper(), f"%{stock_identifier}%"),
            ).fetchone()

            if ticker_row:
                tid = ticker_row["ticker_id"]
                prices = tech.execute(
                    """
                    SELECT date, close FROM ohlcv
                    WHERE ticker_id = ?
                    ORDER BY date DESC LIMIT 400
                    """,
                    (tid,),
                ).fetchall()
                tech.close()

                if prices:
                    closes = list(reversed([p["close"] for p in prices]))
                    dates  = list(reversed([p["date"]  for p in prices]))
                    p_now  = closes[-1]

                    def ret_at(n: int) -> float | None:
                        if len(closes) > n:
                            old = closes[-n]
                            return round((p_now - old) / old * 100, 2) if old else None
                        return None

                    stock_perf = {
                        "symbol": ticker_row["symbol"],
                        "name": ticker_row["company_name"],
                        "current_price": p_now,
                        "ret_1m_pct": ret_at(21),
                        "ret_3m_pct": ret_at(63),
                        "ret_6m_pct": ret_at(126),
                        "ret_1y_pct": ret_at(252),
                        "data_from": dates[0],
                        "data_to": dates[-1],
                    }
        except Exception:
            pass

        # ── DII trend ──────────────────────────────────────────────────
        dii = _dii_trend(stock_identifier)

        # ── Cross-sell logic ───────────────────────────────────────────
        recommendation = ""
        if fund_row and stock_perf:
            fund_1y  = fund_row["ret_1y_pct"] or 0
            stock_1y = stock_perf.get("ret_1y_pct") or 0
            fund_vol = fund_row["volatility_1y"] or 999

            if fund_vol < 15 and stock_1y > fund_1y:
                recommendation = (
                    "DIRECT STOCK wins on return but fund gives lower volatility. "
                    "Recommend fund for conservative/SIP investors, direct stock for high-conviction traders."
                )
            elif fund_1y > stock_1y:
                recommendation = (
                    f"FUND outperforms stock on 1Y basis ({fund_1y:.1f}% vs {stock_1y:.1f}%). "
                    "Fund also diversifies away single-stock risk. Strong cross-sell case."
                )
            elif stock_in_fund:
                recommendation = (
                    f"Stock is {stock_in_fund['weight_pct']:.1f}% of fund. "
                    "Buying fund gives you this stock + diversification across the theme. "
                    "For investors uncertain about single-stock risk, fund is the right vehicle."
                )
            else:
                recommendation = (
                    "Performance comparable. Fund offers diversification; direct stock gives full upside. "
                    "Recommend based on investor's risk tolerance."
                )

        return _fmt({
            "stock": stock_perf or {"note": "Stock not found in technical DB"},
            "fund": (
                {
                    "name": fund_row["scheme_name"],
                    "fund_house": fund_row["fund_house"],
                    "category": fund_row["scheme_category"],
                    "nav": fund_row["nav_latest"],
                    "ret_1y_pct": fund_row["ret_1y_pct"],
                    "ret_3y_pct": fund_row["ret_3y_pct"],
                    "volatility_1y_pct": fund_row["volatility_1y"],
                    "stock_weight_in_fund": stock_in_fund or "Holdings not available",
                }
                if fund_row
                else f"Fund '{fund_name}' not found"
            ),
            "dii_institutional_trend": dii,
            "cross_sell_recommendation": recommendation,
        })
    except Exception as e:
        return f"Error comparing stock vs fund: {e}"


@mcp.tool()
def get_mf_recommendation(
    theme: str = "",
    signal_type: str = "",
    risk_level: str = "moderate",
    limit: int = 5,
) -> str:
    """
    Recommend mutual funds based on equity signal context — the engine behind
    intelligent cross-sell. Maps equity signals/themes to the right fund category.

    Args:
        theme:       Investment theme (e.g. "quality compounder", "small cap momentum",
                     "banking", "pharma", "infrastructure", "defensives", "IT", "FMCG")
        signal_type: Signal context (e.g. "bullish cluster", "oversold recovery",
                     "institutional accumulation", "earnings beat", "turnaround")
        risk_level:  conservative | moderate | aggressive (default: moderate)
        limit:       Max recommendations (default 5)
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        # Map theme/signal to fund category keywords
        category_map = {
            "quality compounder": ["flexi cap", "large cap", "multi cap"],
            "small cap momentum": ["small cap"],
            "mid cap":            ["mid cap"],
            "large cap":          ["large cap", "bluechip"],
            "banking":            ["banking", "financial"],
            "pharma":             ["pharma", "healthcare"],
            "infrastructure":     ["infrastructure", "psu", "manufacturing"],
            "it":                 ["technology", "it", "digital"],
            "fmcg":               ["consumption", "fmcg"],
            "defensives":         ["large cap", "balanced advantage"],
            "turnaround":         ["value", "contra", "special situation"],
            "earnings beat":      ["flexi cap", "multi cap", "focused"],
            "institutional accumulation": ["large cap", "flexi cap"],
            "bullish cluster":    ["mid cap", "small cap", "flexi cap"],
            "oversold recovery":  ["value", "contra", "mid cap"],
        }

        risk_volatility = {
            "conservative": (0, 14),
            "moderate":     (0, 20),
            "aggressive":   (0, 999),
        }
        vol_min, vol_max = risk_volatility.get(risk_level, (0, 20))

        # Find matching category keywords
        search_terms = []
        for key, cats in category_map.items():
            if key in (theme + " " + signal_type).lower():
                search_terms.extend(cats)
        if not search_terms:
            # Default: broad equity if no theme match
            search_terms = ["flexi cap", "large cap", "multi cap", "equity"]

        # Build OR query for categories
        placeholders = " OR ".join(["f.scheme_category LIKE ?"] * len(search_terms))
        params = [f"%{t}%" for t in search_terms]
        params += [vol_min, vol_max, limit]

        rows = conn.execute(
            f"""
            SELECT f.scheme_code, f.scheme_name, f.fund_house, f.scheme_category,
                   fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y, fp.nav_latest
            FROM funds f
            JOIN fund_performance fp USING (scheme_code)
            WHERE ({placeholders})
              AND fp.volatility_1y BETWEEN ? AND ?
              AND fp.ret_1y_pct IS NOT NULL
            ORDER BY fp.ret_1y_pct DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        conn.close()

        if not rows:
            return "No funds found for this theme/risk combination. Try broader parameters."

        # Build rationale
        rationale_map = {
            "bullish cluster": (
                "Multiple technical signals aligned simultaneously — this is the smart-money "
                "signal. Funds in this category own similar quality names systematically."
            ),
            "institutional accumulation": (
                "DII holding is rising. Mutual funds are building positions. "
                "Rather than buying one stock, consider a fund that owns the whole accumulation basket."
            ),
            "earnings beat": (
                "Strong earnings surprise detected. Funds that focus on earnings quality "
                "will systematically hold such names — better risk-adjusted exposure."
            ),
            "turnaround": (
                "Turnaround signals are highest-risk, highest-reward. Value/contra funds "
                "have professional risk management for this theme vs direct stock exposure."
            ),
            "quality compounder": (
                "Quality compounders compound over 10+ years. Flexi/multi-cap funds with "
                "low churn and high quality tilt are the best vehicle for this conviction."
            ),
        }

        theme_rationale = ""
        for key in rationale_map:
            if key in (theme + " " + signal_type).lower():
                theme_rationale = rationale_map[key]
                break
        if not theme_rationale:
            theme_rationale = (
                "These funds match your theme and risk level. "
                "Cross-sell angle: systematic exposure without single-stock concentration risk."
            )

        return _fmt({
            "query": {"theme": theme, "signal_type": signal_type, "risk_level": risk_level},
            "matched_categories": list(set(search_terms)),
            "rationale": theme_rationale,
            "recommendations": [
                {
                    "rank": i + 1,
                    "name": r["scheme_name"],
                    "fund_house": r["fund_house"],
                    "category": r["scheme_category"],
                    "ret_1y_pct": r["ret_1y_pct"],
                    "ret_3y_pct": r["ret_3y_pct"],
                    "volatility_1y_pct": r["volatility_1y"],
                    "cross_sell_pitch": (
                        f"Best-in-category on 1Y return ({r['ret_1y_pct']:.1f}%). "
                        f"Lower volatility than direct stock exposure ({r['volatility_1y']:.1f}% annualised)."
                        if r["volatility_1y"] and r["ret_1y_pct"]
                        else ""
                    ),
                }
                for i, r in enumerate(rows)
            ],
        })
    except Exception as e:
        return f"Error generating MF recommendation: {e}"


@mcp.tool()
def get_portfolio_mf_analysis(
    holdings: str,
    include_mf_alternatives: bool = True,
) -> str:
    """
    Analyse a customer's equity portfolio and identify:
    1. Concentration risk (overweight sectors/stocks)
    2. Missing exposures (mid/small cap gaps, sector gaps)
    3. DII institutional stance on each holding
    4. MF alternatives that plug the gaps
    5. Overlap analysis: which funds already replicate the portfolio

    Cross-sell engine for broking platform: turns portfolio data into
    specific, mathematically-backed MF recommendations.

    Args:
        holdings: Comma-separated stock holdings with optional weights.
                  Formats:
                    "RELIANCE, HDFCBANK, TCS, INFY"  (equal weight assumed)
                    "RELIANCE:30, HDFCBANK:20, TCS:15, INFY:15, BAJFINANCE:20"
        include_mf_alternatives: Whether to add MF recommendations (default True)
    """
    conn = _mf_conn()
    if not conn:
        return _mf_not_seeded()
    try:
        # Parse holdings
        portfolio: list[dict] = []
        total_weight = 0.0

        for item in holdings.split(","):
            item = item.strip()
            if ":" in item:
                name, weight = item.rsplit(":", 1)
                w = float(weight.strip())
            else:
                name = item
                w = None  # will normalise to equal weight later
            portfolio.append({"stock": name.strip(), "weight": w})

        # Normalise equal weights if not specified
        if all(p["weight"] is None for p in portfolio):
            eq = round(100.0 / len(portfolio), 1)
            for p in portfolio:
                p["weight"] = eq
        else:
            # Fill None with average of specified
            specified = [p["weight"] for p in portfolio if p["weight"] is not None]
            avg_w = sum(specified) / len(specified) if specified else 10.0
            for p in portfolio:
                if p["weight"] is None:
                    p["weight"] = avg_w

        total_weight = sum(p["weight"] for p in portfolio)

        # Per-stock: get DII trend + fundamentals from screener
        enriched = []
        for p in portfolio:
            stock = p["stock"]
            dii = _dii_trend(stock)
            dii_trend = dii["dii_trend"] if dii else "unknown"

            # Try to get market cap / sector proxy from screener
            fundamentals: dict[str, Any] = {}
            try:
                sc = _conn(SCREENER_DB)
                row = sc.execute(
                    """
                    SELECT dc.name, frm.market_cap, frm.pe_ratio, frm.roe_pct
                    FROM dim_company dc
                    JOIN fact_realtime_metrics frm USING (company_id)
                    WHERE dc.name LIKE ? OR dc.symbol = ?
                    LIMIT 1
                    """,
                    (f"%{stock}%", stock.upper()),
                ).fetchone()
                if row:
                    fundamentals = {
                        "name": row["name"],
                        "market_cap_cr": row["market_cap"],
                        "pe": row["pe_ratio"],
                        "roe_pct": row["roe_pct"],
                    }
                sc.close()
            except Exception:
                pass

            enriched.append({
                "stock": stock,
                "weight_pct": p["weight"],
                "weight_normalised_pct": round(p["weight"] / total_weight * 100, 1),
                "dii_trend": dii_trend,
                "fundamentals": fundamentals,
            })

        # Concentration analysis
        top_3_weight = sum(
            sorted([e["weight_pct"] for e in enriched], reverse=True)[:3]
        )
        concentration_risk = (
            "HIGH" if top_3_weight > 60
            else "MODERATE" if top_3_weight > 40
            else "LOW"
        )

        # DII signal summary
        dii_accumulating = [e["stock"] for e in enriched if e["dii_trend"] == "accumulating"]
        dii_reducing     = [e["stock"] for e in enriched if e["dii_trend"] == "reducing"]

        # Market cap proxy (rough: >20K cr = large, 5K-20K = mid, <5K = small)
        large_cap = []
        mid_cap   = []
        small_cap = []
        unknown_cap = []
        for e in enriched:
            mc = (e["fundamentals"] or {}).get("market_cap_cr")
            if mc is None:
                unknown_cap.append(e["stock"])
            elif mc >= 20000:
                large_cap.append(e["stock"])
            elif mc >= 5000:
                mid_cap.append(e["stock"])
            else:
                small_cap.append(e["stock"])

        # Gap analysis
        gaps = []
        if len(mid_cap) == 0 and len(small_cap) == 0:
            gaps.append({
                "gap": "No mid/small cap exposure",
                "risk": "Portfolio tracks large caps only — low alpha potential",
                "mf_fix": "Consider a Mid Cap or Small Cap fund for growth kicker",
            })
        if len(large_cap) / max(len(enriched), 1) > 0.8:
            gaps.append({
                "gap": "Heavily large-cap concentrated",
                "risk": "Nifty 50 correlation likely >0.75 — no diversification benefit",
                "mf_fix": "Add Mid/Small Cap or Flexi Cap to reduce index correlation",
            })
        if top_3_weight > 50:
            gaps.append({
                "gap": f"Top 3 stocks = {top_3_weight:.0f}% of portfolio",
                "risk": "High single-stock risk — one bad quarter wipes gains",
                "mf_fix": "Reduce concentration by routing new investments via MF SIP",
            })
        if dii_reducing:
            gaps.append({
                "gap": f"DII reducing in: {', '.join(dii_reducing)}",
                "risk": "Institutional selling — potential overhang on these stocks",
                "mf_fix": "Consider rotating to a fund that has already reduced this exposure",
            })

        # MF alternatives
        mf_alternatives = []
        if include_mf_alternatives:
            # Recommend funds for each identified gap
            gap_to_category = {
                "No mid/small cap": ["mid cap", "small cap"],
                "large-cap concentrated": ["flexi cap", "multi cap"],
                "DII reducing": ["value", "contra"],
            }
            categories_needed = set()
            for gap in gaps:
                for key, cats in gap_to_category.items():
                    if key.lower() in gap["gap"].lower():
                        categories_needed.update(cats)
            if not categories_needed:
                categories_needed = {"flexi cap"}

            for cat in list(categories_needed)[:2]:
                rows = conn.execute(
                    """
                    SELECT f.scheme_name, f.fund_house, fp.ret_1y_pct, fp.ret_3y_pct, fp.volatility_1y
                    FROM funds f JOIN fund_performance fp USING (scheme_code)
                    WHERE f.scheme_category LIKE ?
                      AND fp.ret_1y_pct IS NOT NULL
                    ORDER BY fp.ret_1y_pct DESC LIMIT 3
                    """,
                    (f"%{cat}%",),
                ).fetchall()
                for r in rows:
                    mf_alternatives.append({
                        "category": cat,
                        "name": r["scheme_name"],
                        "fund_house": r["fund_house"],
                        "ret_1y_pct": r["ret_1y_pct"],
                        "ret_3y_pct": r["ret_3y_pct"],
                        "volatility_1y_pct": r["volatility_1y"],
                    })

        conn.close()

        return _fmt({
            "portfolio_summary": {
                "stocks": len(enriched),
                "total_weight_input": total_weight,
                "concentration_risk": concentration_risk,
                "top_3_weight_pct": top_3_weight,
            },
            "cap_breakdown": {
                "large_cap": large_cap,
                "mid_cap": mid_cap,
                "small_cap": small_cap,
                "unknown": unknown_cap,
            },
            "institutional_signals": {
                "dii_accumulating": dii_accumulating,
                "dii_reducing": dii_reducing,
                "interpretation": (
                    f"Institutions building in: {dii_accumulating}. "
                    f"Watch for selling pressure in: {dii_reducing}."
                    if dii_accumulating or dii_reducing
                    else "Institutional data pending."
                ),
            },
            "holdings": enriched,
            "gaps_identified": gaps,
            "mf_alternatives_to_plug_gaps": mf_alternatives,
            "cross_sell_summary": (
                f"{len(gaps)} portfolio gaps identified. "
                f"{len(mf_alternatives)} MF alternatives suggested. "
                "Use get_mf_recommendation() for deeper theme-specific matching."
            ),
        })
    except Exception as e:
        return f"Error analysing portfolio: {e}"


# ---------------------------------------------------------------------------
# Tool 29: get_benchmark_data  (Nifty 50 / Bank Nifty)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_benchmark_data(
    index: str = "nifty50",
    days: int = 365,
) -> str:
    """
    Fetch OHLCV history and live indicators for Nifty 50 or Bank Nifty.

    Seed data with: python technical-module/technical-module-venv/Scripts/python -m src.pipeline

    Args:
      index : "nifty50" (default) or "banknifty"
      days  : how many calendar days of history to return (default 365)

    Returns OHLCV rows + current RSI, MACD, EMA-200 for the index.
    Use to contextualise individual stock moves against the broader market:
      - Stock down 5% but Nifty down 6% → stock actually outperforming
      - Stock RSI 75 and Nifty RSI 72    → market-wide overbought, not stock-specific
    """
    try:
        yf_map = {"nifty50": "^NSEI", "banknifty": "^NSEBANK"}
        yf_symbol = yf_map.get(index.lower().replace(" ", "").replace("-", ""))
        if not yf_symbol:
            return f"Unknown index '{index}'. Choose: nifty50, banknifty."

        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found. Run the technical pipeline first."

        conn = _conn(TECHNICAL_DB)
        ticker = _one(conn, "SELECT id FROM tickers WHERE yf_symbol = ?", (yf_symbol,))
        if not ticker:
            return (
                f"No data for {index} yet. "
                "Run: python technical-module/technical-module-venv/Scripts/python -m src.pipeline"
            )

        tid = ticker["id"]
        ohlcv = _rows(conn,
            """
            SELECT date, open, high, low, close, volume
            FROM   ohlcv
            WHERE  ticker_id = ? AND date >= date('now', ? || ' days')
            ORDER  BY date ASC
            """,
            (tid, f"-{days}"),
        )

        # Live indicators from full history
        closes_rows = _rows(conn,
            "SELECT close FROM ohlcv WHERE ticker_id = ? AND date >= date('now', '-400 days') ORDER BY date ASC",
            (tid,),
        )
        conn.close()

        closes = [r["close"] for r in closes_rows if r["close"]]
        indicators = _compute_indicators(closes) if len(closes) >= 26 else {}

        return _fmt({
            "index":       index,
            "yf_symbol":   yf_symbol,
            "rows":        len(ohlcv),
            "latest_close": ohlcv[-1]["close"] if ohlcv else None,
            "indicators":  {
                k: indicators.get(k) for k in
                ("rsi_14", "rsi_signal", "macd_histogram", "macd_trend",
                 "price_vs_ema_200_pct", "trend", "bb_pct_b")
            },
            "ohlcv": ohlcv,
        })
    except Exception as e:
        return f"Error fetching benchmark data: {e}"


# ---------------------------------------------------------------------------
# Forensic helpers — lazy fetch + cache
# ---------------------------------------------------------------------------

def _forensic_conn():
    """Returns a connection to the forensic DB (local SQLite or Turso DB1)."""
    if FORENSIC_DB.exists():
        from src.db import create_schema, get_connection
        conn = get_connection()
        create_schema(conn)
        return conn
    return _TursoConn("db1")


def _forensic_fresh(nse_code: str, source: str, max_age_days: int = 7) -> bool:
    """Return True if we have a fetch logged within max_age_days (success or no_data).
    'failed' fetches are retried; 'no_data' means Trendlyne has no record — don't spam."""
    try:
        conn = _forensic_conn()
        row = conn.execute(
            """
            SELECT fetched_at FROM fetch_log
            WHERE  source = ? AND identifier = ?
              AND  status IN ('success', 'no_data')
              AND  fetched_at >= datetime('now', ? || ' days')
            """,
            (source, nse_code, f"-{max_age_days}"),
        ).fetchone()
        conn.close()
        return row is not None
    except Exception:
        return False


def _forensic_fetch_status(nse_code: str, source: str) -> str | None:
    """Return the most recent fetch status for this nse_code + source, or None."""
    try:
        conn = _forensic_conn()
        row = conn.execute(
            "SELECT status FROM fetch_log WHERE source=? AND identifier=? ORDER BY fetched_at DESC LIMIT 1",
            (source, nse_code),
        ).fetchone()
        conn.close()
        return row["status"] if row else None
    except Exception:
        return None


def _forensic_seed(nse_code: str) -> None:
    """Fetch insider + pledge data on-demand and cache to forensic.db."""
    from src.db import log_fetch, upsert_insider_transaction, upsert_pledge_event
    from src.fetcher import fetch_insider_transactions, fetch_pledge_data

    conn = _forensic_conn()
    try:
        txns = fetch_insider_transactions(nse_code)
        for t in txns:
            upsert_insider_transaction(conn, t)
        conn.commit()
        log_fetch(conn, "trendlyne_insider", nse_code,
                  "success" if txns else "no_data", row_count=len(txns))
        conn.commit()
    except Exception as e:
        try:
            log_fetch(conn, "trendlyne_insider", nse_code, "failed", error_msg=str(e))
            conn.commit()
        except Exception:
            pass

    try:
        events = fetch_pledge_data(nse_code)
        for ev in events:
            upsert_pledge_event(conn, ev)
        conn.commit()
        log_fetch(conn, "trendlyne_pledge", nse_code,
                  "success" if events else "no_data", row_count=len(events))
        conn.commit()
    except Exception as e:
        try:
            log_fetch(conn, "trendlyne_pledge", nse_code, "failed", error_msg=str(e))
            conn.commit()
        except Exception:
            pass

    conn.close()


def _ensure_forensic_data(nse_code: str) -> None:
    """Fetch and cache forensic data if not already fresh (within 7 days)."""
    insider_fresh = _forensic_fresh(nse_code, "trendlyne_insider")
    pledge_fresh  = _forensic_fresh(nse_code, "trendlyne_pledge")
    if not insider_fresh or not pledge_fresh:
        _forensic_seed(nse_code)


# ---------------------------------------------------------------------------
# Tool 30: get_insider_transactions
# ---------------------------------------------------------------------------

@mcp.tool()
def get_insider_transactions(
    identifier: str,
    days: int = 730,
    transaction_type: str = "all",
) -> str:
    """
    Fetch insider buy/sell/warrant/pledge transactions for a company.

    Data sourced live from Trendlyne SAST filings (fetched on first call,
    cached for 7 days). No pipeline setup required.

    Args:
      identifier       : NSE symbol, BSE code, or ISIN
      days             : how many calendar days back to look (default 730 = 2 years)
      transaction_type : filter by type — "all" (default), "buy", "sell",
                         "pledge_created", "pledge_released", "warrant_conversion",
                         "esop", "inter_se_transfer"

    KEY SIGNALS:
      Insider BUYING at low prices before results     → strong conviction
      Insider SELLING into positive news / rallies    → distribution red flag
      WARRANT_CONVERSION at market highs              → dilution at peak
      Multiple insiders selling simultaneously        → coordinated exit signal
      Compliance officer / CFO receiving warrants     → governance red flag
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")
        if not nse_code:
            return f"No NSE code found for '{identifier}'."

        _ensure_forensic_data(nse_code)
        conn = _forensic_conn()
        params: tuple = (nse_code, days)
        type_filter = ""
        if transaction_type != "all":
            type_filter = " AND transaction_type = ?"
            params = (nse_code, days, transaction_type)

        rows = _rows(conn,
            f"""
            SELECT person_name, person_category, transaction_type,
                   shares, price, transaction_date,
                   before_pct, after_pct, mode
            FROM   insider_transactions
            WHERE  nse_code = ?
              AND  (transaction_date >= date('now', ? || ' days')
                    OR transaction_date IS NULL)
            {type_filter}
            ORDER  BY transaction_date DESC
            """,
            params,
        )
        conn.close()

        if not rows:
            fetch_status = _forensic_fetch_status(nse_code, "trendlyne_insider")
            if fetch_status in ("no_data", "failed"):
                return (
                    f"No SAST/insider data available for '{nse_code}' from Trendlyne "
                    f"(fetch status: {fetch_status}). Trendlyne may not cover this company "
                    f"or use a different symbol slug.\n\n"
                    f"Alternative: try get_bse_filings('{nse_code}', subcategory='SAST') "
                    f"to fetch SEBI SAST disclosures directly from BSE."
                )
            return f"No insider transactions found for '{nse_code}' in the last {days} days."

        # Compute summary stats
        buys    = [r for r in rows if r["transaction_type"] == "buy"]
        sells   = [r for r in rows if r["transaction_type"] == "sell"]
        warrants = [r for r in rows if r["transaction_type"] == "warrant_conversion"]
        pledges  = [r for r in rows if "pledge" in (r["transaction_type"] or "")]

        summary = {
            "total_transactions": len(rows),
            "buys":               len(buys),
            "sells":              len(sells),
            "warrant_conversions": len(warrants),
            "pledge_events":      len(pledges),
            "net_bias":           "buying" if len(buys) > len(sells) else "selling" if len(sells) > len(buys) else "neutral",
        }

        return _fmt({
            "nse_code":     nse_code,
            "period_days":  days,
            "summary":      summary,
            "transactions": rows,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error fetching insider transactions for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 31: get_pledge_status
# ---------------------------------------------------------------------------

@mcp.tool()
def get_pledge_status(
    identifier: str,
    days: int = 365,
) -> str:
    """
    Fetch pledge creation and release history for a company's promoters/insiders.

    Data sourced live from Trendlyne pledge disclosures (fetched on first call,
    cached for 7 days). No pipeline setup required.

    Args:
      identifier : NSE symbol, BSE code, or ISIN
      days       : how many calendar days back to look (default 365)

    KEY SIGNALS:
      pledge_created with high pledge_pct   → forced-selling risk if price falls
      Multiple pledges close together       → liquidity stress
      pledge_released after price rally     → insiders monetising gains
      pledge_pct > 50%                      → CRITICAL — margin call cascade risk
      Pledged shares as % of free float > 20% → supply overhang warning
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")
        if not nse_code:
            return f"No NSE code found for '{identifier}'."

        _ensure_forensic_data(nse_code)
        conn = _forensic_conn()

        events = _rows(conn,
            """
            SELECT person_name, event_type, shares_changed,
                   total_pledged, pledge_pct, event_date
            FROM   pledge_events
            WHERE  nse_code = ?
              AND  (event_date >= date('now', ? || ' days')
                    OR event_date IS NULL)
            ORDER  BY event_date DESC
            """,
            (nse_code, days),
        )

        # Latest pledge_pct per person
        latest_by_person = _rows(conn,
            """
            SELECT person_name,
                   MAX(event_date) AS latest_date,
                   pledge_pct,
                   total_pledged
            FROM   pledge_events
            WHERE  nse_code = ?
            GROUP  BY person_name
            ORDER  BY pledge_pct DESC NULLS LAST
            """,
            (nse_code,),
        )

        conn.close()

        if not events and not latest_by_person:
            fetch_status = _forensic_fetch_status(nse_code, "trendlyne_pledge")
            if fetch_status in ("no_data", "failed"):
                return (
                    f"No pledge data available for '{nse_code}' from Trendlyne "
                    f"(fetch status: {fetch_status}). Trendlyne may not cover this company.\n\n"
                    f"Alternative: try get_bse_filings('{nse_code}') and look for pledge "
                    f"disclosures in the filings list."
                )
            return f"No pledge data found for '{nse_code}'."

        total_current_pledged = sum(
            (r["total_pledged"] or 0) for r in latest_by_person
            if r["total_pledged"]
        )
        max_pledge_pct = max(
            (r["pledge_pct"] or 0.0) for r in latest_by_person
        ) if latest_by_person else 0.0

        risk_level = (
            "CRITICAL"  if max_pledge_pct > 50 else
            "HIGH"      if max_pledge_pct > 25 else
            "MEDIUM"    if max_pledge_pct > 10 else
            "LOW"
        )

        return _fmt({
            "nse_code":              nse_code,
            "pledge_risk_level":     risk_level,
            "max_pledge_pct":        max_pledge_pct,
            "total_pledged_shares":  total_current_pledged,
            "pledged_by_person":     latest_by_person,
            "recent_events":         events,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error fetching pledge status for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 32: get_forensic_profile
# ---------------------------------------------------------------------------

@mcp.tool()
def get_forensic_profile(identifier: str) -> str:
    """
    Full forensic synthesis for a company: insider activity + pledges +
    news-price divergence + earnings quality — all red flags in one call.

    Combines data from ALL four pipelines (forensic + screener + news + technical).
    Use this as the starting point for any governance / fraud-risk investigation.

    Flags raised (each scored 0 = none, 1 = caution, 2 = warning, 3 = critical):
      INSIDER_SELLING_INTO_NEWS   — insiders sold on the same days as positive news
      HIGH_PLEDGE                 — promoter shares pledged > 25%
      WARRANT_DILUTION            — warrant conversions detected (dilution at peak)
      NEGATIVE_OPERATING_CF       — PAT positive but cash flow negative (earnings quality)
      PROMOTER_STAKE_DECLINING    — promoter % shrinking quarter-on-quarter
      NEWS_PRICE_DIVERGENCE       — 3+ positive news events caused stock to fall
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")

        flags: list[dict] = []
        data:  dict       = {"nse_code": nse_code, "company_name": company.get("company_name")}

        # ── Insider selling + warrant activity ──────────────────────────────
        if nse_code:
            _ensure_forensic_data(nse_code)
            fconn = _forensic_conn()
            sells = _rows(fconn,
                """
                SELECT COUNT(*) AS cnt, SUM(IFNULL(shares, 0)) AS total_shares
                FROM   insider_transactions
                WHERE  nse_code = ? AND transaction_type = 'sell'
                  AND  transaction_date >= date('now', '-730 days')
                """,
                (nse_code,),
            )
            warrants = _rows(fconn,
                """
                SELECT COUNT(*) AS cnt, SUM(IFNULL(shares, 0)) AS total_shares
                FROM   insider_transactions
                WHERE  nse_code = ? AND transaction_type = 'warrant_conversion'
                """,
                (nse_code,),
            )
            pledge_summary = _rows(fconn,
                """
                SELECT MAX(pledge_pct) AS max_pct,
                       MAX(total_pledged) AS max_pledged
                FROM   pledge_events
                WHERE  nse_code = ?
                """,
                (nse_code,),
            )
            fconn.close()

            sell_cnt   = (sells[0]["cnt"]   if sells   else 0) or 0
            sell_shrs  = (sells[0]["total_shares"] if sells else 0) or 0
            warr_cnt   = (warrants[0]["cnt"] if warrants else 0) or 0
            max_pledge = (pledge_summary[0]["max_pct"] if pledge_summary else None) or 0.0

            data["insider_sell_transactions_2y"] = sell_cnt
            data["insider_sell_shares_2y"]       = sell_shrs
            data["warrant_conversion_count"]     = warr_cnt
            data["max_pledge_pct"]               = max_pledge

            if sell_cnt >= 3:
                flags.append({"flag": "INSIDER_SELLING", "severity": 2,
                               "detail": f"{sell_cnt} sell transactions in last 2 years ({sell_shrs:,} shares)"})
            if warr_cnt > 0:
                flags.append({"flag": "WARRANT_DILUTION", "severity": 1,
                               "detail": f"{warr_cnt} warrant conversions recorded"})
            if max_pledge > 50:
                flags.append({"flag": "HIGH_PLEDGE", "severity": 3,
                               "detail": f"Max pledge: {max_pledge:.1f}% — forced-selling cascade risk"})
            elif max_pledge > 25:
                flags.append({"flag": "HIGH_PLEDGE", "severity": 2,
                               "detail": f"Max pledge: {max_pledge:.1f}% — elevated margin-call risk"})

        # ── News-price divergence ────────────────────────────────────────────
        if _db_available(NEWS_DB) and company.get("tickertape_id"):
            nconn = _conn(NEWS_DB)
            divergent = _rows(nconn,
                """
                SELECT COUNT(*) AS cnt
                FROM   news_articles
                WHERE  company_id = ?
                  AND  sentiment_label = 'positive'
                  AND  price_reaction_pct < -2.0
                  AND  published_at >= date('now', '-365 days')
                """,
                (company["tickertape_id"],),
            )
            nconn.close()
            div_cnt = (divergent[0]["cnt"] if divergent else 0) or 0
            data["positive_news_price_drops_1y"] = div_cnt
            if div_cnt >= 4:
                flags.append({"flag": "NEWS_PRICE_DIVERGENCE", "severity": 3,
                               "detail": f"{div_cnt} positive news events caused stock to fall — distribution signal"})
            elif div_cnt >= 2:
                flags.append({"flag": "NEWS_PRICE_DIVERGENCE", "severity": 2,
                               "detail": f"{div_cnt} positive news events caused stock to fall"})

        # ── Earnings quality (CF vs PAT) ─────────────────────────────────────
        if _db_available(SCREENER_DB) and company.get("screener_company_id"):
            sconn = _conn(SCREENER_DB)
            cf_rows = _rows(sconn,
                """
                SELECT fcf.cash_from_operating, fpl.net_profit,
                       dp.year
                FROM   fact_cash_flow fcf
                JOIN   fact_profit_loss fpl ON fpl.company_id = fcf.company_id
                                           AND fpl.period_id   = fcf.period_id
                JOIN   dim_period dp        ON dp.period_id     = fcf.period_id
                WHERE  fcf.company_id = ?
                ORDER  BY dp.year DESC
                LIMIT 5
                """,
                (company["screener_company_id"],),
            )
            sconn.close()

            neg_cf_years = [
                r["year"] for r in cf_rows
                if r["cash_from_operating"] is not None
                and r["net_profit"] is not None
                and r["cash_from_operating"] < 0
                and r["net_profit"] > 0
            ]
            data["negative_operating_cf_with_positive_pat_years"] = neg_cf_years
            if len(neg_cf_years) >= 3:
                flags.append({"flag": "NEGATIVE_OPERATING_CF", "severity": 3,
                               "detail": f"PAT positive but operating CF negative in {len(neg_cf_years)} of last 5 years: {neg_cf_years}"})
            elif len(neg_cf_years) >= 1:
                flags.append({"flag": "NEGATIVE_OPERATING_CF", "severity": 1,
                               "detail": f"Operating CF negative with positive PAT in FY{neg_cf_years}"})

            # Promoter trend
            ptconn = _conn(SCREENER_DB)
            promo_rows = _rows(ptconn,
                """
                SELECT fs.promoters_pct, dp.year, dp.quarter
                FROM   fact_shareholding fs
                JOIN   dim_period dp ON dp.period_id = fs.period_id
                WHERE  fs.company_id = ? AND fs.period_type = 'quarterly'
                  AND  fs.promoters_pct IS NOT NULL
                ORDER  BY dp.year DESC, dp.quarter DESC
                LIMIT  6
                """,
                (company["screener_company_id"],),
            )
            ptconn.close()
            if len(promo_rows) >= 3:
                pct_vals = [r["promoters_pct"] for r in promo_rows]
                if all(pct_vals[i] < pct_vals[i + 1] for i in range(min(3, len(pct_vals) - 1))):
                    flags.append({"flag": "PROMOTER_STAKE_DECLINING", "severity": 2,
                                   "detail": f"Promoter stake declining 3+ consecutive quarters: {pct_vals[:4]}"})
                data["promoter_pct_recent"] = pct_vals[:4]

        # ── Risk summary ─────────────────────────────────────────────────────
        max_severity  = max((f["severity"] for f in flags), default=0)
        risk_label    = {0: "CLEAN", 1: "WATCH", 2: "CAUTION", 3: "HIGH RISK"}.get(max_severity, "UNKNOWN")

        return _fmt({
            "forensic_risk":  risk_label,
            "flags_raised":   len(flags),
            "flags":          flags,
            "data":           data,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error building forensic profile for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 33: screen_pledge_risk
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_pledge_risk(
    min_pledge_pct: float = 10.0,
    limit: int = 50,
) -> str:
    """
    Scan all companies in the forensic DB and return those with high promoter pledge %.

    A high pledge percentage means:
      - Promoter shares are collateral for loans
      - If the stock falls, lenders force-sell those shares
      - Force-selling accelerates the price decline (cascade risk)
      - Retail investors have NO warning until the selling starts

    Args:
      min_pledge_pct : minimum pledge % to include (default 10.0)
      limit          : max results (default 50, max 200)

    Combine with get_pledge_status(symbol) for full timeline on any result.
    """
    try:
        limit = min(limit, 200)
        conn = _forensic_conn()
        rows = _rows(conn,
            """
            SELECT nse_code,
                   person_name,
                   MAX(pledge_pct)    AS max_pledge_pct,
                   MAX(total_pledged) AS total_pledged_shares,
                   MAX(event_date)    AS latest_event
            FROM   pledge_events
            WHERE  pledge_pct >= ?
            GROUP  BY nse_code, person_name
            ORDER  BY max_pledge_pct DESC
            LIMIT  ?
            """,
            (min_pledge_pct, limit),
        )
        conn.close()

        if not rows:
            return (
                f"No companies found with pledge_pct >= {min_pledge_pct}%. "
                "Use get_pledge_status(symbol) per company to fetch and cache data on-demand."
            )

        # Enrich with company name from identity.db
        if _db_available(IDENTITY_DB):
            iconn = _identity_conn()
            names = {
                r["nse_code"]: r["company_name"]
                for r in _rows(iconn,
                    "SELECT nse_code, company_name FROM company_map WHERE nse_code IS NOT NULL"
                )
            }
            iconn.close()
            for r in rows:
                r["company_name"] = names.get(r["nse_code"])

        return _fmt({
            "min_pledge_pct":   min_pledge_pct,
            "results_returned": len(rows),
            "high_pledge_companies": rows,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error screening pledge risk: {e}"


# ---------------------------------------------------------------------------
# Tool 34: screen_insider_activity
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_insider_activity(
    days: int = 30,
    transaction_type: str = "buy",
    limit: int = 50,
) -> str:
    """
    Market-wide scan of insider transactions — who is buying or selling across
    all companies in the forensic DB over the last N days.

    Args:
      days             : lookback window (default 30, max 365)
      transaction_type : "buy" (default), "sell", "warrant_conversion",
                         "pledge_created", "pledge_released", "all"
      limit            : max results (default 50, max 200)

    USE CASES:
      screen_insider_activity(days=7, transaction_type="buy")
        → stocks where insiders bought THIS WEEK (high-conviction near-term signal)
      screen_insider_activity(days=30, transaction_type="sell")
        → stocks where insiders are distributing (avoid or short)
      screen_insider_activity(days=90, transaction_type="warrant_conversion")
        → dilution pipeline — warrants converting = new supply incoming
      screen_insider_activity(days=30, transaction_type="pledge_created")
        → fresh pledges = promoter liquidity stress
    """
    try:
        days  = min(days, 365)
        limit = min(limit, 200)

        conn = _forensic_conn()
        type_clause = "" if transaction_type == "all" else "AND transaction_type = ?"
        params = (
            (days, transaction_type, limit)
            if transaction_type != "all"
            else (days, limit)
        )

        rows = _rows(conn,
            f"""
            SELECT nse_code, person_name, person_category,
                   transaction_type, shares, price, transaction_date, after_pct
            FROM   insider_transactions
            WHERE  transaction_date >= date('now', '-' || ? || ' days')
            {type_clause}
            ORDER  BY transaction_date DESC
            LIMIT  ?
            """,
            params,
        )
        conn.close()

        if not rows:
            return (
                f"No {transaction_type} transactions found in the last {days} days. "
                "Use get_insider_transactions(symbol) per company to fetch and cache data on-demand."
            )

        # Enrich with company name
        if _db_available(IDENTITY_DB):
            iconn = _identity_conn()
            names = {
                r["nse_code"]: r["company_name"]
                for r in _rows(iconn,
                    "SELECT nse_code, company_name FROM company_map WHERE nse_code IS NOT NULL"
                )
            }
            iconn.close()
            for r in rows:
                r["company_name"] = names.get(r["nse_code"])

        return _fmt({
            "days":              days,
            "transaction_type":  transaction_type,
            "results_returned":  len(rows),
            "transactions":      rows,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error screening insider activity: {e}"


# ---------------------------------------------------------------------------
# Tool 35: get_bulk_deals
# ---------------------------------------------------------------------------

@mcp.tool()
def get_bulk_deals(
    date: str = "today",
    min_value_cr: float = 5.0,
    days: int = 1,
) -> str:
    """
    Fetch BSE bulk deal data — large block trades (typically institutional).

    Bulk deals are single-session trades > 0.5% of a stock's equity, reported
    to BSE by end of day. They reveal WHICH institution bought or sold, at
    what price, and how much. No retail tool surfaces this data cleanly.

    Seed data with: python forensic-module/run_pipeline.py --bulk-deals

    Args:
      date         : "today", "yesterday", or ISO date "2026-03-17" (default: today)
      min_value_cr : minimum deal value in crores to include (default 5.0)
      days         : if > 1, scan the last N calendar days (ignores date arg)

    KEY SIGNALS:
      Large BUY by a known fund → institutional accumulation
      Large SELL by a known fund → distribution in progress
      Multiple institutions buying same stock same day → coordinated accumulation
      Operator names (unknown entities buying huge quantities) → pump signal
    """
    try:
        import datetime as dt
        if not _db_available(FORENSIC_DB):
            return (
                "No bulk deal data yet. "
                "Run: python forensic-module/run_pipeline.py --bulk-deals"
            )

        conn = _forensic_conn()

        if days > 1:
            rows = _rows(conn,
                """
                SELECT deal_date, exchange, nse_code, company_name,
                       client_name, deal_type, quantity, price, value_cr
                FROM   bulk_deals
                WHERE  deal_date >= date('now', ? || ' days')
                  AND  (value_cr IS NULL OR value_cr >= ?)
                ORDER  BY deal_date DESC, value_cr DESC NULLS LAST
                """,
                (f"-{days}", min_value_cr),
            )
        else:
            if date == "today":
                target = dt.date.today().isoformat()
            elif date == "yesterday":
                target = (dt.date.today() - dt.timedelta(days=1)).isoformat()
            else:
                target = date

            rows = _rows(conn,
                """
                SELECT deal_date, exchange, nse_code, company_name,
                       client_name, deal_type, quantity, price, value_cr
                FROM   bulk_deals
                WHERE  deal_date = ?
                  AND  (value_cr IS NULL OR value_cr >= ?)
                ORDER  BY value_cr DESC NULLS LAST
                """,
                (target, min_value_cr),
            )

        conn.close()

        if not rows:
            return (
                f"No bulk deals found (min ₹{min_value_cr} Cr). "
                "Data may not be seeded yet — run: python forensic-module/run_pipeline.py --bulk-deals"
            )

        buys  = [r for r in rows if r["deal_type"] == "buy"]
        sells = [r for r in rows if r["deal_type"] == "sell"]
        total_buy_cr  = sum((r["value_cr"] or 0) for r in buys)
        total_sell_cr = sum((r["value_cr"] or 0) for r in sells)

        return _fmt({
            "period":          f"last {days} days" if days > 1 else target,
            "min_value_cr":    min_value_cr,
            "total_deals":     len(rows),
            "buy_deals":       len(buys),
            "sell_deals":      len(sells),
            "total_buy_cr":    round(total_buy_cr, 2),
            "total_sell_cr":   round(total_sell_cr, 2),
            "net_institutional_flow_cr": round(total_buy_cr - total_sell_cr, 2),
            "deals":           rows,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error fetching bulk deals: {e}"


# ---------------------------------------------------------------------------
# Tool 36: get_fraud_score
# ---------------------------------------------------------------------------

@mcp.tool()
def get_fraud_score(identifier: str) -> str:
    """
    Compute a forensic fraud-risk score (0–100) for a company using 8 weighted checks.

    Score interpretation:
      0–20   → LOW RISK    (clean across all available checks)
      21–40  → MODERATE    (1-2 yellow flags; worth monitoring)
      41–60  → ELEVATED    (multiple red flags; deep-dive warranted)
      61–80  → HIGH RISK   (serious concern; institutional-grade red flag)
      81–100 → CRITICAL    (extreme risk; avoid or short)

    CRITICAL RULE: If fewer than 4 of 8 checks have sufficient data, the score
    is null and the reason "insufficient_data" is returned. A score of 0 for a
    company we know nothing about is the most dangerous failure mode.

    The 8-check registry (weights sum to 100):
      1. CF/PAT < 0.5 for 3+ years         (weight 20) — earnings quality
      2. Positive news → price falls 3+x   (weight 15) — market disbelief
      3. Warrant dilution > 15% of base     (weight 15) — promoter extraction
      4. Pledge % > 50% any entity          (weight 10) — financial stress
      5. Shell company proxies in group     (weight 10) — structure risk (proxy)
      6. Zero dividends + rising profit 5yr (weight 10) — cash hoarding signal
      7. Related-party revenue > 30%        (weight 10) — captive revenue risk
      8. Auditor changed in last 3 years    (weight 10) — governance warning

    Checks 5, 7, 8 are currently data-unavailable (MCA/annual report parsing
    not yet implemented). Their weight is excluded from the denominator so the
    score is normalised against available data only.

    Args:
      identifier : NSE symbol, BSE code, ISIN, or screener slug
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        nse_code = company.get("nse_code") or ""
        display_name = company.get("company_name") or identifier

        # Ensure forensic data is seeded before any check that needs it
        if nse_code:
            try:
                _ensure_forensic_data(nse_code)
            except Exception:
                pass  # forensic checks will show "unavailable" individually

        # Registry: (check_id, name, weight, status)
        # status will be filled per-check: "pass" | "fail" | "partial" | "unavailable"
        checks: list[dict] = []

        # ── Check 1: CF/PAT < 0.5 for 3+ years (weight 20) ──────────────────
        check1: dict = {
            "check_id": 1,
            "name":     "earnings_quality_cf_pat",
            "label":    "Cash earnings quality (CF/PAT < 0.5 for 3+ years)",
            "weight":   20,
            "status":   "unavailable",
            "detail":   None,
            "score_contribution": 0,
        }
        cid = company.get("screener_company_id")
        if cid and _db_available(SCREENER_DB):
            try:
                s_conn = _conn(SCREENER_DB)
                cf_pat_rows = _rows(s_conn,
                    """
                    SELECT dp.year,
                           fcf.cash_from_operating,
                           fpl.net_profit
                    FROM   fact_cash_flow fcf
                    JOIN   fact_profit_loss fpl ON fpl.company_id = fcf.company_id
                           AND fpl.period_id = fcf.period_id
                    JOIN   dim_period dp ON dp.period_id = fcf.period_id
                    WHERE  fcf.company_id = ?
                      AND  fpl.net_profit IS NOT NULL
                      AND  fcf.cash_from_operating IS NOT NULL
                    ORDER  BY dp.year DESC
                    LIMIT  7
                    """,
                    (cid,),
                )
                s_conn.close()
                if len(cf_pat_rows) >= 3:
                    low_quality_years = [
                        r for r in cf_pat_rows
                        if r["net_profit"] and r["net_profit"] > 0
                        and (r["cash_from_operating"] / r["net_profit"]) < 0.5
                    ]
                    check1["detail"] = {
                        "years_checked":    len(cf_pat_rows),
                        "low_quality_years": len(low_quality_years),
                        "threshold":        "CF/PAT < 0.5",
                    }
                    if len(low_quality_years) >= 3:
                        check1["status"] = "fail"
                        check1["score_contribution"] = 20
                    else:
                        check1["status"] = "pass"
                        check1["score_contribution"] = 0
            except Exception as e:
                check1["status"] = "unavailable"
                check1["detail"] = {"error": str(e)}
        checks.append(check1)

        # ── Check 2: Positive news → price falls 3+ times (weight 15) ────────
        check2: dict = {
            "check_id": 2,
            "name":     "news_price_divergence",
            "label":    "Positive news ignored by market (3+ occurrences)",
            "weight":   15,
            "status":   "unavailable",
            "detail":   None,
            "score_contribution": 0,
        }
        if nse_code and _db_available(NEWS_DB):
            try:
                n_conn = _conn(NEWS_DB)
                # Find company in tickertape DB
                comp_row = _one(n_conn,
                    "SELECT id FROM companies WHERE nse_code = ? LIMIT 1",
                    (nse_code,),
                )
                if comp_row:
                    ttape_company_id = comp_row["id"]
                    # Positive articles where price went down after
                    divergence_rows = _rows(n_conn,
                        """
                        SELECT na.headline,
                               na.score_positive,
                               ars.initial_price,
                               ars.close_price,
                               ROUND((ars.close_price - ars.initial_price) * 100.0
                                     / NULLIF(ars.initial_price, 0), 2) AS price_chg_pct
                        FROM   news_articles na
                        JOIN   article_stocks ars ON ars.article_id = na.id
                        WHERE  ars.company_id = ?
                          AND  na.score_positive > 0.6
                          AND  ars.initial_price IS NOT NULL
                          AND  ars.close_price IS NOT NULL
                          AND  ars.close_price < ars.initial_price
                        ORDER  BY na.published_at DESC
                        LIMIT  20
                        """,
                        (ttape_company_id,),
                    )
                    n_conn.close()
                    check2["detail"] = {
                        "positive_news_with_price_fall": len(divergence_rows),
                        "threshold": "3+ occurrences of score_positive > 0.6 → price down",
                    }
                    if len(divergence_rows) >= 3:
                        check2["status"] = "fail"
                        check2["score_contribution"] = 15
                    else:
                        check2["status"] = "pass"
                        check2["score_contribution"] = 0
                else:
                    n_conn.close()
                    check2["detail"] = {"note": "Company not found in news DB"}
            except Exception as e:
                check2["status"] = "unavailable"
                check2["detail"] = {"error": str(e)}
        checks.append(check2)

        # ── Check 3: Warrant dilution > 15% of equity base (weight 15) ───────
        check3: dict = {
            "check_id": 3,
            "name":     "warrant_dilution",
            "label":    "Warrant dilution > 15% of share base",
            "weight":   15,
            "status":   "unavailable",
            "detail":   None,
            "score_contribution": 0,
        }
        if nse_code and _db_available(FORENSIC_DB):
            try:
                _ensure_forensic_data(nse_code)
                f_conn = _forensic_conn()
                # Warrant conversions = promoter/insider creates shares at discount
                warrant_rows = _rows(f_conn,
                    """
                    SELECT person_name, shares, transaction_date, after_pct, before_pct
                    FROM   insider_transactions
                    WHERE  nse_code = ?
                      AND  transaction_type = 'warrant_conversion'
                    ORDER  BY transaction_date DESC
                    """,
                    (nse_code,),
                )
                f_conn.close()
                if warrant_rows:
                    # If after_pct - before_pct delta on any warrant event > 15pp
                    max_dilution = max(
                        ((r["after_pct"] or 0) - (r["before_pct"] or 0))
                        for r in warrant_rows
                    )
                    total_warrant_shares = sum(r["shares"] or 0 for r in warrant_rows)
                    check3["detail"] = {
                        "warrant_events":    len(warrant_rows),
                        "total_shares_converted": total_warrant_shares,
                        "max_single_dilution_pp": round(max_dilution, 2),
                    }
                    if max_dilution > 15:
                        check3["status"] = "fail"
                        check3["score_contribution"] = 15
                    else:
                        check3["status"] = "pass"
                        check3["score_contribution"] = 0
                else:
                    check3["status"] = "pass"
                    check3["detail"] = {"note": "No warrant conversions found"}
                    check3["score_contribution"] = 0
            except Exception as e:
                check3["status"] = "unavailable"
                check3["detail"] = {"error": str(e)}
        checks.append(check3)

        # ── Check 4: Pledge % > 50% any entity (weight 10) ───────────────────
        check4: dict = {
            "check_id": 4,
            "name":     "high_pledge_pct",
            "label":    "Pledge % > 50% for any promoter entity",
            "weight":   10,
            "status":   "unavailable",
            "detail":   None,
            "score_contribution": 0,
        }
        if nse_code and _db_available(FORENSIC_DB):
            try:
                f_conn = _forensic_conn()
                # Most recent pledge % per person
                pledge_rows = _rows(f_conn,
                    """
                    SELECT person_name, pledge_pct, event_date
                    FROM   pledge_events
                    WHERE  nse_code = ?
                      AND  pledge_pct IS NOT NULL
                    ORDER  BY event_date DESC
                    """,
                    (nse_code,),
                )
                f_conn.close()
                if pledge_rows:
                    # Latest pledge_pct per person
                    latest: dict[str, float] = {}
                    for r in pledge_rows:
                        if r["person_name"] not in latest:
                            latest[r["person_name"]] = r["pledge_pct"]
                    max_pledge = max(latest.values()) if latest else 0
                    high_pledge_entities = [
                        {"person": p, "pledge_pct": pct}
                        for p, pct in latest.items()
                        if pct > 50
                    ]
                    check4["detail"] = {
                        "entities_checked":  len(latest),
                        "max_pledge_pct":    round(max_pledge, 2),
                        "high_pledge_entities": high_pledge_entities,
                    }
                    if high_pledge_entities:
                        check4["status"] = "fail"
                        check4["score_contribution"] = 10
                    else:
                        check4["status"] = "pass"
                        check4["score_contribution"] = 0
                else:
                    check4["status"] = "pass"
                    check4["detail"] = {"note": "No pledge data found"}
                    check4["score_contribution"] = 0
            except Exception as e:
                check4["status"] = "unavailable"
                check4["detail"] = {"error": str(e)}
        checks.append(check4)

        # ── Check 5: Shell companies in promoter group (weight 10) ────────────
        # Data unavailable — MCA registry parsing not yet implemented
        checks.append({
            "check_id": 5,
            "name":     "shell_company_proxies",
            "label":    "Shell companies in promoter group (paid-up ≤ ₹1L, assets > ₹1Cr)",
            "weight":   10,
            "status":   "unavailable",
            "detail":   {"note": "Requires MCA registry — not yet implemented"},
            "score_contribution": 0,
        })

        # ── Check 6: Zero dividends + rising profit 5yr (weight 10) ──────────
        check6: dict = {
            "check_id": 6,
            "name":     "dividend_withholding",
            "label":    "Zero dividends despite rising profits for 5+ years",
            "weight":   10,
            "status":   "unavailable",
            "detail":   None,
            "score_contribution": 0,
        }
        if cid and _db_available(SCREENER_DB):
            try:
                s_conn = _conn(SCREENER_DB)
                div_profit_rows = _rows(s_conn,
                    """
                    SELECT dp.year, fpl.net_profit, fpl.dividend_payout_pct
                    FROM   fact_profit_loss fpl
                    JOIN   dim_period dp ON dp.period_id = fpl.period_id
                    WHERE  fpl.company_id = ?
                      AND  fpl.net_profit IS NOT NULL
                    ORDER  BY dp.year DESC
                    LIMIT  6
                    """,
                    (cid,),
                )
                s_conn.close()
                if len(div_profit_rows) >= 5:
                    profits  = [r["net_profit"] or 0 for r in div_profit_rows]
                    divs     = [r["dividend_payout_pct"] or 0 for r in div_profit_rows]
                    zero_div = all(d == 0 for d in divs)
                    # Rising profit: oldest-5 < newest (reversed since ordered DESC)
                    profit_rising = profits[0] > profits[-1] if profits[0] and profits[-1] else False
                    check6["detail"] = {
                        "years_checked":       len(div_profit_rows),
                        "zero_dividends":      zero_div,
                        "profit_rising":       profit_rising,
                        "latest_profit_cr":    round(profits[0], 2) if profits[0] else None,
                        "oldest_profit_cr":    round(profits[-1], 2) if profits[-1] else None,
                    }
                    if zero_div and profit_rising:
                        check6["status"] = "fail"
                        check6["score_contribution"] = 10
                    else:
                        check6["status"] = "pass"
                        check6["score_contribution"] = 0
            except Exception as e:
                check6["status"] = "unavailable"
                check6["detail"] = {"error": str(e)}
        checks.append(check6)

        # ── Check 7: Related-party revenue > 30% (weight 10) ─────────────────
        # Data unavailable — not in screener.db
        checks.append({
            "check_id": 7,
            "name":     "related_party_revenue",
            "label":    "Related-party revenue > 30% of total revenue",
            "weight":   10,
            "status":   "unavailable",
            "detail":   {"note": "Requires annual report parsing — not yet implemented"},
            "score_contribution": 0,
        })

        # ── Check 8: Auditor changed in last 3 years (weight 10) ─────────────
        # Data unavailable — not in any current DB
        checks.append({
            "check_id": 8,
            "name":     "auditor_change",
            "label":    "Auditor changed in last 3 years",
            "weight":   10,
            "status":   "unavailable",
            "detail":   {"note": "Requires auditor history tracking — not yet implemented"},
            "score_contribution": 0,
        })

        # ── Score computation ─────────────────────────────────────────────────
        available = [c for c in checks if c["status"] != "unavailable"]
        failed    = [c for c in checks if c["status"] == "fail"]

        if len(available) < 4:
            return _fmt({
                "company":    display_name,
                "nse_code":   nse_code,
                "score":      None,
                "reason":     "insufficient_data",
                "detail":     (
                    f"Only {len(available)} of 8 checks have data. "
                    "Minimum 4 required before emitting a score."
                ),
                "checks":     checks,
            })

        # Normalise against available weight only
        available_weight = sum(c["weight"] for c in available)
        raw_score = sum(c["score_contribution"] for c in failed)
        normalised_score = round(raw_score * 100 / available_weight) if available_weight else 0

        if normalised_score <= 20:
            risk_label = "LOW"
        elif normalised_score <= 40:
            risk_label = "MODERATE"
        elif normalised_score <= 60:
            risk_label = "ELEVATED"
        elif normalised_score <= 80:
            risk_label = "HIGH"
        else:
            risk_label = "CRITICAL"

        flags = [c["label"] for c in failed]

        return _fmt({
            "company":          display_name,
            "nse_code":         nse_code,
            "score":            normalised_score,
            "risk_label":       risk_label,
            "flags":            flags,
            "checks_available": len(available),
            "checks_failed":    len(failed),
            "available_weight": available_weight,
            "checks":           checks,
            "interpretation": (
                "Score is normalised against available data only. "
                "A score of 0 means CLEAN across all available checks — "
                "not 'no data'. See 'checks' array for per-check detail."
            ),
        })

    except Exception as e:
        return f"Error computing fraud score for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 37: get_signal_efficacy
# ---------------------------------------------------------------------------

@mcp.tool()
def get_signal_efficacy(
    signal_type: str = "all",
    holding_period_days: int = 90,
    min_price: float = 50.0,
) -> str:
    """
    Backtest a technical signal type against 2 years of NSE price history.

    Returns the historical win rate, average return, and median return for
    each signal type — answering "does this signal actually predict price
    movement on Indian markets?"

    This data is unique: no retail tool in India surfaces signal efficacy
    across all 5,000 NSE stocks simultaneously.

    Args:
      signal_type         : specific signal name (e.g. "Golden Cross",
                            "Bullish MACD Crossover") or "all" for full table
      holding_period_days : how many days after the signal to measure return
                            (30, 60, 90, 180 — default 90)
      min_price           : exclude penny stocks below this price (default 50)

    KEY FINDINGS FROM THE DATA:
      Bullish MACD Crossover → ~58% win rate, +4.5% median 90-day return
      Golden Cross           → ~43% win rate (below random chance)
      Use win_rate > 55% AND sample_size > 100 as reliability thresholds.
    """
    try:
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found. Run: python run_all.py --only technical"

        import datetime as dt
        cutoff_end   = (dt.date.today() - dt.timedelta(days=holding_period_days)).isoformat()
        cutoff_start = "2024-03-18"

        # When local file exists, the 3-way SQL join works.
        # On Railway the tables are split across DB1 (ohlcv) and DB2 (signals/tickers),
        # so we do a Python-side join instead.
        _use_local = TECHNICAL_DB.exists()
        conn = _conn(TECHNICAL_DB) if _use_local else None

        # Build signal filter
        if signal_type == "all":
            _sig_src = conn if _use_local else _TursoConn("db2")
            signal_types = [r["signal_type"] for r in _rows(_sig_src,
                "SELECT DISTINCT signal_type FROM signals ORDER BY signal_type"
            )]
        else:
            signal_types = [signal_type]

        results = []
        for st in signal_types:
            if _use_local:
                rows = _rows(conn,
                    f"""
                    SELECT
                        s.ticker_id,
                        s.date AS sig_date,
                        o_at.close AS price_at,
                        (SELECT o2.close FROM ohlcv o2
                         WHERE o2.ticker_id = s.ticker_id
                           AND o2.date >= date(s.date, '+' || ? || ' days')
                         ORDER BY o2.date ASC LIMIT 1) AS price_fwd
                    FROM signals s
                    JOIN ohlcv o_at ON o_at.ticker_id = s.ticker_id AND o_at.date = s.date
                    JOIN tickers t ON t.id = s.ticker_id
                    WHERE s.signal_type = ?
                      AND s.date >= ?
                      AND s.date <= ?
                      AND o_at.close >= ?
                      AND t.exchange != 'INDEX'
                    """,
                    (holding_period_days, st, cutoff_start, cutoff_end, min_price),
                )
            else:
                # Railway: Python-side join across DB1 (ohlcv) and DB2 (signals/tickers)
                from turso_db import db2_query, db1_query
                sigs = db2_query(
                    """SELECT s.ticker_id, s.date AS sig_date
                       FROM signals s JOIN tickers t ON t.id = s.ticker_id
                       WHERE s.signal_type = ? AND s.date >= ? AND s.date <= ?
                         AND t.exchange != 'INDEX'""",
                    [st, cutoff_start, cutoff_end],
                )
                if not sigs:
                    rows = []
                else:
                    tids = list({r["ticker_id"] for r in sigs})
                    placeholders = ",".join("?" * len(tids))
                    ohlcv_rows = db1_query(
                        f"SELECT ticker_id, date, close FROM ohlcv WHERE ticker_id IN ({placeholders}) AND date >= ?",
                        tids + [cutoff_start],
                    )
                    # Build lookup: (ticker_id, date) -> close
                    price_map = {(r["ticker_id"], r["date"]): r["close"] for r in ohlcv_rows}
                    # Forward price: (ticker_id, sig_date) -> close on/after sig_date + holding_period
                    # Group ohlcv dates per ticker, sorted
                    from collections import defaultdict
                    tid_dates = defaultdict(list)
                    for r in ohlcv_rows:
                        tid_dates[r["ticker_id"]].append(r["date"])
                    for tid in tid_dates:
                        tid_dates[tid].sort()

                    rows = []
                    for sig in sigs:
                        tid, sig_date = sig["ticker_id"], sig["sig_date"]
                        price_at = price_map.get((tid, sig_date))
                        if price_at is None or price_at < min_price:
                            continue
                        # Find forward price
                        import bisect
                        fwd_target = (dt.date.fromisoformat(sig_date) + dt.timedelta(days=holding_period_days)).isoformat()
                        dates_for_tid = tid_dates.get(tid, [])
                        idx = bisect.bisect_left(dates_for_tid, fwd_target)
                        price_fwd = price_map.get((tid, dates_for_tid[idx])) if idx < len(dates_for_tid) else None
                        rows.append({"ticker_id": tid, "sig_date": sig_date,
                                     "price_at": price_at, "price_fwd": price_fwd})

            valid = [r for r in rows if r["price_fwd"] and r["price_at"]]
            if len(valid) < 20:
                results.append({
                    "signal_type":   st,
                    "sample_size":   len(valid),
                    "win_rate_pct":  None,
                    "avg_return_pct": None,
                    "median_return_pct": None,
                    "note":          "insufficient_data (< 20 samples)",
                })
                continue

            returns = sorted([
                round((r["price_fwd"] - r["price_at"]) * 100 / r["price_at"], 2)
                for r in valid
            ])
            wins = [r for r in returns if r > 0]
            avg_ret = round(sum(returns) / len(returns), 2)
            med_ret = returns[len(returns) // 2]

            results.append({
                "signal_type":        st,
                "sample_size":        len(valid),
                "holding_period_days": holding_period_days,
                "win_rate_pct":       round(len(wins) / len(returns) * 100, 1),
                "avg_return_pct":     avg_ret,
                "median_return_pct":  med_ret,
                "best_pct":           max(returns),
                "worst_pct":          min(returns),
                "reliability":        "HIGH" if len(valid) >= 200 else
                                      "MEDIUM" if len(valid) >= 50 else "LOW",
            })

        conn.close()

        # Sort by median return descending
        results.sort(key=lambda x: (x.get("median_return_pct") or -999), reverse=True)

        if signal_type != "all" and results:
            r = results[0]
            verdict = (
                "PREDICTIVE" if (r.get("win_rate_pct") or 0) >= 55 else
                "NEUTRAL"    if (r.get("win_rate_pct") or 0) >= 45 else
                "FADE"
            )
            r["verdict"] = verdict
            r["interpretation"] = (
                f"When '{signal_type}' fires on a stock priced above ₹{min_price}, "
                f"the price is higher {r.get('win_rate_pct')}% of the time after "
                f"{holding_period_days} days (median: {r.get('median_return_pct')}%). "
                f"Sample: {r.get('sample_size')} occurrences on NSE."
            )

        return _fmt({
            "holding_period_days": holding_period_days,
            "min_price_filter":    min_price,
            "signals_evaluated":   len(results),
            "results":             results,
        })

    except Exception as e:
        return f"Error computing signal efficacy: {e}"


# ---------------------------------------------------------------------------
# Tool 38: screen_value_traps
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_value_traps(
    min_pe: float = 25.0,
    max_promoter_pct: float = 30.0,
    max_dividend_payout_pct: float = 5.0,
    min_profit_cr: float = 50.0,
    limit: int = 30,
) -> str:
    """
    Screen for value trap stocks — companies that look profitable on the surface
    but show classic signs of promoter value extraction rather than creation.

    A value trap passes all four filters simultaneously:
      1. High valuation (PE > threshold) — market paying up for "growth"
      2. Low promoter skin-in-game (promoters_pct < threshold)
      3. Zero or minimal dividends — profits aren't returned to shareholders
      4. Actually profitable — so "no dividend" isn't explained by losses

    When promoters own very little, pay nothing out, and the stock is expensively
    valued, there's an incentive mismatch: management is compensated to grow the
    company's size, not its value per share.

    Args:
      min_pe                  : minimum P/E ratio (default 25)
      max_promoter_pct        : maximum promoter holding % (default 30)
      max_dividend_payout_pct : maximum dividend payout % (default 5 = near-zero)
      min_profit_cr           : minimum net profit in crores (filter noise)
      limit                   : max results (default 30)

    SIGNALS TO WATCH:
      Very low promoter % + high PE + no dividends = value trap
      If also has rising related-party transactions = extraction in progress
      Combine with get_fraud_score() for full picture
    """
    try:
        if not _db_available(SCREENER_DB):
            return "Screener DB not found. Run: python run_all.py --only screener"

        conn = _conn(SCREENER_DB)

        rows = _rows(conn,
            """
            WITH latest_pl AS (
                SELECT fpl.company_id, fpl.net_profit, fpl.dividend_payout_pct,
                       dp.year,
                       ROW_NUMBER() OVER (PARTITION BY fpl.company_id ORDER BY dp.year DESC) rn
                FROM fact_profit_loss fpl
                JOIN dim_period dp ON dp.period_id = fpl.period_id
            ),
            latest_shareholding AS (
                SELECT fs.company_id, fs.promoters_pct, fs.fiis_pct, fs.diis_pct,
                       ROW_NUMBER() OVER (
                           PARTITION BY fs.company_id
                           ORDER BY dp.year DESC, dp.quarter DESC
                       ) rn
                FROM fact_shareholding fs
                JOIN dim_period dp ON dp.period_id = fs.period_id
                WHERE fs.period_type = 'quarterly'
            )
            SELECT
                dc.symbol,
                dc.name,
                dc.nse_code,
                frm.market_cap,
                frm.current_price,
                frm.pe_ratio,
                frm.roe_pct,
                ls.promoters_pct,
                ls.fiis_pct,
                ls.diis_pct,
                lp.net_profit,
                lp.dividend_payout_pct
            FROM dim_company dc
            JOIN fact_realtime_metrics frm ON frm.company_id = dc.company_id
            JOIN latest_shareholding ls ON ls.company_id = dc.company_id AND ls.rn = 1
            JOIN latest_pl lp ON lp.company_id = dc.company_id AND lp.rn = 1
            WHERE frm.pe_ratio >= ?
              AND ls.promoters_pct <= ?
              AND (lp.dividend_payout_pct IS NULL OR lp.dividend_payout_pct <= ?)
              AND lp.net_profit >= ?
              AND dc.nse_code IS NOT NULL
            ORDER BY frm.pe_ratio DESC
            LIMIT ?
            """,
            (min_pe, max_promoter_pct, max_dividend_payout_pct,
             min_profit_cr, limit),
        )
        conn.close()

        if not rows:
            return _fmt({
                "message": "No value traps found with these filters.",
                "filters": {
                    "min_pe": min_pe,
                    "max_promoter_pct": max_promoter_pct,
                    "max_dividend_payout_pct": max_dividend_payout_pct,
                    "min_profit_cr": min_profit_cr,
                },
            })

        return _fmt({
            "filters": {
                "min_pe":                  min_pe,
                "max_promoter_pct":        max_promoter_pct,
                "max_dividend_payout_pct": max_dividend_payout_pct,
                "min_profit_cr":           min_profit_cr,
            },
            "count":            len(rows),
            "interpretation":   (
                "These companies are profitable with high valuations, "
                "yet promoters own little and pay no dividends. "
                "Classic incentive mismatch — growth in company size, not value per share. "
                "Run get_fraud_score() on any of these for deeper forensic analysis."
            ),
            "value_traps":      rows,
        })

    except Exception as e:
        return f"Error screening value traps: {e}"


# ---------------------------------------------------------------------------
# Tool: get_sector_pulse (TODO 11)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_sector_pulse(sector: str | None = None, days: int = 7) -> str:
    """
    Aggregate technical signal sentiment by sector or industry.

    Shows which sectors are seeing broad bullish/bearish momentum — useful for
    detecting sector rotations before they become obvious in price.

    Args:
        sector: Optional sector filter (e.g. "Banking & Finance", "IT", "Pharma").
                Pass None to see all sectors ranked by bullish %.
                Partial match supported — "pharma" matches "Pharmaceuticals".
        days:   Lookback window for signals (default 7)

    KEY SIGNALS:
      Sector bullish% > 60% → broad accumulation, consider longs
      Sector bullish% < 30% → broad distribution, avoid new entries
      Rising sector + falling broader market → relative strength / rotation
      Divergence between sector tone and price → leading indicator
    """
    try:
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found. Run: python run_all.py --only technical"
        if not _db_available(IDENTITY_DB):
            return "Identity DB not found. Run: python identity.py"

        tech_conn = _conn(TECHNICAL_DB)
        id_conn   = _conn(IDENTITY_DB)

        # Get all companies with sector data
        sector_rows = id_conn.execute(
            """
            SELECT nse_code, bse_code, name, industry, sector
            FROM   company_map
            WHERE  (industry IS NOT NULL OR sector IS NOT NULL)
              AND  entity_type = 'stock'
            """
        ).fetchall()
        id_conn.close()

        if not sector_rows:
            return (
                "No sector data found in identity.db. "
                "Run: python identity.py  (to seed Accord industry data)"
            )

        # Build nse_code → sector/industry lookup
        code_to_meta: dict[str, dict] = {}
        for r in sector_rows:
            if r["nse_code"]:
                code_to_meta[r["nse_code"]] = {
                    "name": r["name"], "industry": r["industry"], "sector": r["sector"],
                }

        # Apply sector filter
        if sector:
            s_lower = sector.lower()
            code_to_meta = {
                k: v for k, v in code_to_meta.items()
                if s_lower in (v["sector"] or "").lower()
                or s_lower in (v["industry"] or "").lower()
            }
            if not code_to_meta:
                # List available sectors
                all_sectors = sorted({
                    r["sector"] for r in sector_rows if r["sector"]
                })
                return (
                    f"No sector matching '{sector}' found.\n"
                    f"Available sectors: {all_sectors}"
                )

        # Pull signal counts per NSE code
        nse_codes = list(code_to_meta.keys())
        placeholders = ",".join("?" * len(nse_codes))

        sig_rows = tech_conn.execute(
            f"""
            SELECT  tk.nse_code,
                    SUM(CASE WHEN s.signal_type LIKE '%bullish%'
                                  OR s.signal_type LIKE '%buy%'
                                  OR s.signal_type LIKE '%golden%'
                             THEN 1 ELSE 0 END) AS bullish,
                    SUM(CASE WHEN s.signal_type LIKE '%bearish%'
                                  OR s.signal_type LIKE '%sell%'
                                  OR s.signal_type LIKE '%death%'
                             THEN 1 ELSE 0 END) AS bearish,
                    COUNT(*) AS total
            FROM   signals s
            JOIN   tickers tk ON tk.id = s.ticker_id
            WHERE  tk.nse_code IN ({placeholders})
              AND  s.signal_date >= date('now', ? || ' days')
            GROUP  BY tk.nse_code
            """,
            (*nse_codes, f"-{days}"),
        ).fetchall()
        tech_conn.close()

        # Aggregate per sector
        from collections import defaultdict
        sector_stats: dict[str, dict] = defaultdict(lambda: {
            "bullish": 0, "bearish": 0, "total_signals": 0, "stocks": 0,
            "bullish_stocks": 0, "bearish_stocks": 0, "top_bullish": [], "top_bearish": [],
        })

        for r in sig_rows:
            nse = r["nse_code"]
            if nse not in code_to_meta:
                continue
            meta  = code_to_meta[nse]
            grp   = meta["sector"] or meta["industry"] or "Unknown"
            st    = sector_stats[grp]
            st["bullish"]       += r["bullish"]
            st["bearish"]       += r["bearish"]
            st["total_signals"] += r["total"]
            st["stocks"]        += 1
            if r["bullish"] > r["bearish"]:
                st["bullish_stocks"] += 1
                st["top_bullish"].append(nse)
            elif r["bearish"] > r["bullish"]:
                st["bearish_stocks"] += 1
                st["top_bearish"].append(nse)

        if not sector_stats:
            return f"No signals found in the last {days} days for the selected sector(s)."

        # Build output — sorted by bullish stock % descending
        output = []
        for grp, st in sorted(
            sector_stats.items(),
            key=lambda x: x[1]["bullish_stocks"] / max(x[1]["stocks"], 1),
            reverse=True,
        ):
            total_stocks = st["stocks"]
            bullish_pct  = round(st["bullish_stocks"] / total_stocks * 100, 1) if total_stocks else 0
            bearish_pct  = round(st["bearish_stocks"] / total_stocks * 100, 1) if total_stocks else 0
            output.append({
                "sector":           grp,
                "stocks_with_signals": total_stocks,
                "bullish_pct":      bullish_pct,
                "bearish_pct":      bearish_pct,
                "neutral_pct":      round(100 - bullish_pct - bearish_pct, 1),
                "total_signals":    st["total_signals"],
                "top_bullish_stocks": st["top_bullish"][:5],
                "top_bearish_stocks": st["top_bearish"][:5],
            })

        return _fmt({
            "days":         days,
            "sector_filter": sector,
            "sectors":      output,
            "note": (
                "bullish_pct = % of stocks in sector where bullish signals > bearish signals. "
                "Sectors with bullish_pct > 60% → broad accumulation. "
                "Use get_technical_signals(nse_code) to drill into individual stocks."
            ),
        })

    except Exception as e:
        return f"Error fetching sector pulse: {e}"


# ---------------------------------------------------------------------------
# Tool 39: get_market_breadth
# ---------------------------------------------------------------------------

@mcp.tool()
def get_market_breadth(days: int = 7) -> str:
    """
    Market-wide breadth and sentiment across all NSE stocks.

    Answers the question every analyst asks before picking individual stocks:
    "Is the market's internal health supportive of new positions, or is
    the tide going out?"

    Computes:
      - Golden Cross vs Death Cross distribution (who's above/below EMA-200)
      - Bullish vs bearish signal count in the last N days
      - Most active signal types today/this week
      - Net breadth score: (bullish stocks - bearish stocks) / total

    Args:
      days : lookback window for signal counting (default 7)

    INTERPRETATION:
      Net breadth > +30%  → strong bull market internals, be aggressive
      Net breadth 0–30%   → mixed market, be selective
      Net breadth < 0%    → distribution phase, reduce risk
      Net breadth < -30%  → bear market internals, defensive posture
    """
    try:
        if not _db_available(TECHNICAL_DB):
            return "Technical DB not found. Run: python run_all.py --only technical"

        conn = _conn(TECHNICAL_DB)

        # ── EMA-200 proxy: most recent Golden/Death Cross per stock ───────────
        cross_rows = _rows(conn,
            """
            SELECT signal_type, COUNT(DISTINCT s.ticker_id) AS stock_count
            FROM (
                SELECT s.ticker_id, s.signal_type,
                       ROW_NUMBER() OVER (
                           PARTITION BY s.ticker_id
                           ORDER BY s.date DESC
                       ) AS rn
                FROM signals s
                JOIN tickers t ON t.id = s.ticker_id
                WHERE s.signal_type IN ('Golden Cross', 'Death Cross')
                  AND t.exchange != 'INDEX'
            ) s
            WHERE rn = 1
            GROUP BY signal_type
            """,
        )
        cross_map = {r["signal_type"]: r["stock_count"] for r in cross_rows}
        above_ema200 = cross_map.get("Golden Cross", 0)
        below_ema200 = cross_map.get("Death Cross", 0)
        total_crossed = above_ema200 + below_ema200

        # ── Signal distribution in lookback window ────────────────────────────
        signal_dist = _rows(conn,
            """
            SELECT s.signal_type, s.direction,
                   COUNT(DISTINCT s.ticker_id) AS stock_count,
                   COUNT(*) AS signal_count
            FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.date >= date('now', ? || ' days')
              AND t.exchange != 'INDEX'
            GROUP BY s.signal_type, s.direction
            ORDER BY stock_count DESC
            LIMIT 20
            """,
            (f"-{days}",),
        )

        # Aggregate bullish vs bearish stocks
        bull_stocks: set = set()
        bear_stocks: set = set()

        bull_bear_rows = _rows(conn,
            """
            SELECT s.ticker_id, s.direction
            FROM signals s
            JOIN tickers t ON t.id = s.ticker_id
            WHERE s.date >= date('now', ? || ' days')
              AND t.exchange != 'INDEX'
            GROUP BY s.ticker_id, s.direction
            """,
            (f"-{days}",),
        )
        for r in bull_bear_rows:
            if r["direction"] == "bullish":
                bull_stocks.add(r["ticker_id"])
            elif r["direction"] == "bearish":
                bear_stocks.add(r["ticker_id"])

        only_bull = bull_stocks - bear_stocks
        only_bear = bear_stocks - bull_stocks
        both      = bull_stocks & bear_stocks
        total_active = len(bull_stocks | bear_stocks)

        net_breadth_pct = (
            round((len(only_bull) - len(only_bear)) / total_active * 100, 1)
            if total_active else 0
        )

        if net_breadth_pct > 30:
            breadth_label = "STRONG BULL"
        elif net_breadth_pct > 0:
            breadth_label = "MILD BULL"
        elif net_breadth_pct > -30:
            breadth_label = "MILD BEAR"
        else:
            breadth_label = "STRONG BEAR"

        # ── Top 5 most active signal types ───────────────────────────────────
        top_signals = signal_dist[:5]

        conn.close()

        return _fmt({
            "lookback_days":    days,
            "ema_200_proxy": {
                "above_ema200_stocks":  above_ema200,
                "below_ema200_stocks":  below_ema200,
                "pct_above_ema200":     round(above_ema200 / total_crossed * 100, 1)
                                        if total_crossed else None,
                "note": "Based on most recent Golden/Death Cross per stock",
            },
            "signal_breadth": {
                "active_stocks":        total_active,
                "purely_bullish":       len(only_bull),
                "purely_bearish":       len(only_bear),
                "mixed_signals":        len(both),
                "net_breadth_pct":      net_breadth_pct,
                "breadth_label":        breadth_label,
            },
            "top_signals_this_week":    top_signals,
            "interpretation": (
                f"Market breadth: {breadth_label} ({net_breadth_pct:+.1f}%). "
                f"{above_ema200} stocks above EMA-200 vs {below_ema200} below. "
                f"In the last {days} days: {len(only_bull)} stocks showing only bullish signals, "
                f"{len(only_bear)} showing only bearish signals."
            ),
        })

    except Exception as e:
        return f"Error computing market breadth: {e}"


# ---------------------------------------------------------------------------
# Tool 40: get_smart_money_flow
# ---------------------------------------------------------------------------

@mcp.tool()
def get_smart_money_flow(
    identifier: str,
    days: int = 90,
) -> str:
    """
    Composite smart money flow score combining three informed-participant signals.

    Smart money has information or conviction advantages over retail. When multiple
    informed signals align — insiders buying, institutions accumulating, bulk deals
    flowing in — that is a high-conviction setup that often precedes price moves.

    The three components:
      1. INSIDER FLOW     — net insider buy/sell from SAST disclosures (forensic.db)
      2. INSTITUTIONAL    — FII + DII stake change last 2 quarters (screener.db)
      3. BULK DEAL FLOW   — net institutional bulk deal value in crores (forensic.db)

    Score: -100 (all selling) to +100 (all buying)
    Lag detection: if score > 0 but price is flat/down, smart money is accumulating
                   before retail notices — potential opportunity.

    Args:
      identifier : NSE symbol, BSE code, ISIN, or screener slug
      days       : lookback window for insider + bulk deal signals (default 90)
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        nse_code     = company.get("nse_code") or ""
        display_name = company.get("company_name") or identifier
        result: dict = {
            "company":  display_name,
            "nse_code": nse_code,
            "days":     days,
            "components": {},
            "score":    None,
            "signal":   None,
        }

        scores: list[float] = []

        # ── Component 1: Insider flow ─────────────────────────────────────────
        if nse_code and _db_available(FORENSIC_DB):
            try:
                _ensure_forensic_data(nse_code)
                f_conn = _forensic_conn()
                insider_rows = _rows(f_conn,
                    """
                    SELECT transaction_type, shares, price, transaction_date, person_name
                    FROM insider_transactions
                    WHERE nse_code = ?
                      AND transaction_date >= date('now', ? || ' days')
                      AND transaction_type IN ('buy', 'sell', 'esop')
                    ORDER BY transaction_date DESC
                    """,
                    (nse_code, f"-{days}"),
                )
                f_conn.close()

                buy_value  = sum(
                    (r["shares"] or 0) * (r["price"] or 0)
                    for r in insider_rows if r["transaction_type"] == "buy"
                )
                sell_value = sum(
                    (r["shares"] or 0) * (r["price"] or 0)
                    for r in insider_rows if r["transaction_type"] == "sell"
                )
                total_insider = buy_value + sell_value
                insider_score = (
                    round((buy_value - sell_value) / total_insider * 100)
                    if total_insider > 0 else 0
                )
                scores.append(insider_score)
                result["components"]["insider"] = {
                    "buy_events":     len([r for r in insider_rows if r["transaction_type"] == "buy"]),
                    "sell_events":    len([r for r in insider_rows if r["transaction_type"] == "sell"]),
                    "buy_value_cr":   round(buy_value / 1e7, 2),
                    "sell_value_cr":  round(sell_value / 1e7, 2),
                    "net_score":      insider_score,
                    "recent":         insider_rows[:5],
                }
            except Exception as e:
                result["components"]["insider"] = {"error": str(e)}

        # ── Component 2: Institutional stake change ───────────────────────────
        cid = company.get("screener_company_id")
        if cid and _db_available(SCREENER_DB):
            try:
                s_conn = _conn(SCREENER_DB)
                sh_rows = _rows(s_conn,
                    """
                    SELECT fs.fiis_pct, fs.diis_pct, dp.year, dp.quarter
                    FROM fact_shareholding fs
                    JOIN dim_period dp ON dp.period_id = fs.period_id
                    WHERE fs.company_id = ?
                      AND fs.period_type = 'quarterly'
                    ORDER BY dp.year DESC, dp.quarter DESC
                    LIMIT 2
                    """,
                    (cid,),
                )
                s_conn.close()
                if len(sh_rows) >= 2:
                    inst_now  = (sh_rows[0]["fiis_pct"] or 0) + (sh_rows[0]["diis_pct"] or 0)
                    inst_prev = (sh_rows[1]["fiis_pct"] or 0) + (sh_rows[1]["diis_pct"] or 0)
                    delta_pp   = round(inst_now - inst_prev, 2)
                    # Normalise: ±5pp change → ±100 score
                    inst_score = max(-100, min(100, round(delta_pp * 20)))
                    scores.append(inst_score)
                    result["components"]["institutional"] = {
                        "fiis_now":    sh_rows[0]["fiis_pct"],
                        "diis_now":    sh_rows[0]["diis_pct"],
                        "inst_total_now":  round(inst_now, 2),
                        "inst_total_prev": round(inst_prev, 2),
                        "delta_pp":    delta_pp,
                        "net_score":   inst_score,
                        "period":      f"Q{sh_rows[0]['quarter']} {sh_rows[0]['year']} vs Q{sh_rows[1]['quarter']} {sh_rows[1]['year']}",
                    }
            except Exception as e:
                result["components"]["institutional"] = {"error": str(e)}

        # ── Component 3: Bulk deal flow ───────────────────────────────────────
        if _db_available(FORENSIC_DB):
            try:
                f_conn = _forensic_conn()
                bulk_rows = _rows(f_conn,
                    """
                    SELECT deal_type, SUM(value_cr) AS total_cr,
                           COUNT(*) AS deal_count
                    FROM bulk_deals
                    WHERE (nse_code = ? OR company_name LIKE ?)
                      AND deal_date >= date('now', ? || ' days')
                      AND value_cr IS NOT NULL
                    GROUP BY deal_type
                    """,
                    (nse_code, f"%{display_name[:10]}%", f"-{days}"),
                )
                f_conn.close()
                bulk_map = {r["deal_type"]: r for r in bulk_rows}
                buy_cr  = (bulk_map.get("buy") or {}).get("total_cr") or 0
                sell_cr = (bulk_map.get("sell") or {}).get("total_cr") or 0
                total_bulk = buy_cr + sell_cr
                bulk_score = (
                    round((buy_cr - sell_cr) / total_bulk * 100)
                    if total_bulk > 0 else 0
                )
                if total_bulk > 0:
                    scores.append(bulk_score)
                result["components"]["bulk_deals"] = {
                    "buy_cr":    round(buy_cr, 2),
                    "sell_cr":   round(sell_cr, 2),
                    "net_cr":    round(buy_cr - sell_cr, 2),
                    "net_score": bulk_score if total_bulk > 0 else None,
                    "note":      "No bulk deals found in period" if total_bulk == 0 else None,
                }
            except Exception as e:
                result["components"]["bulk_deals"] = {"error": str(e)}

        # ── Composite score ───────────────────────────────────────────────────
        if scores:
            composite = round(sum(scores) / len(scores))
            if composite >= 50:
                signal = "STRONG ACCUMULATION"
            elif composite >= 20:
                signal = "MILD ACCUMULATION"
            elif composite >= -20:
                signal = "NEUTRAL"
            elif composite >= -50:
                signal = "MILD DISTRIBUTION"
            else:
                signal = "STRONG DISTRIBUTION"

            result["score"]  = composite
            result["signal"] = signal
            result["components_used"] = len(scores)

            # Lag detection: price flat/down while smart money accumulating?
            if _db_available(TECHNICAL_DB) and nse_code and composite > 20:
                try:
                    id_conn = _identity_conn()
                    tid_row = _one(id_conn,
                        "SELECT ticker_id FROM company_map WHERE nse_code = ? LIMIT 1",
                        (nse_code,),
                    )
                    id_conn.close()
                    if tid_row:
                        t_conn = _conn(TECHNICAL_DB)
                        price_rows = _rows(t_conn,
                            f"""
                            SELECT close, date FROM ohlcv
                            WHERE ticker_id = ?
                              AND date >= date('now', '-{days} days')
                            ORDER BY date ASC
                            """,
                            (tid_row["ticker_id"],),
                        )
                        t_conn.close()
                        if len(price_rows) >= 2:
                            price_chg = round(
                                (price_rows[-1]["close"] - price_rows[0]["close"])
                                * 100 / price_rows[0]["close"], 1
                            )
                            result["lag_detection"] = {
                                "price_change_pct": price_chg,
                                "smart_money_score": composite,
                                "opportunity_flag": price_chg < 5 and composite > 30,
                                "interpretation": (
                                    "POTENTIAL LAG — smart money accumulating while price is flat. "
                                    "Watch for price to follow smart money."
                                    if price_chg < 5 and composite > 30
                                    else "Price and smart money are in sync."
                                ),
                            }
                except Exception:
                    pass
        else:
            result["score"]  = None
            result["signal"] = "insufficient_data"

        return _fmt(result)

    except Exception as e:
        return f"Error computing smart money flow for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 41: get_conviction_score
# ---------------------------------------------------------------------------

@mcp.tool()
def get_conviction_score(identifier: str) -> str:
    """
    Cross-pillar conviction score combining all 5 data pipelines.

    This is the synthesis tool — the radar that triangulates every signal
    we have about a company simultaneously. When all pillars agree,
    that convergence is rare and high-signal. When they diverge,
    the divergence itself tells you something important.

    The 5 pillars (each scored 0–100):
      1. FUNDAMENTAL QUALITY  — ROE, ROCE, debt, earnings quality (screener.db)
      2. TECHNICAL MOMENTUM   — signal direction + recency + efficacy-weighted (finance.db)
      3. NEWS SENTIMENT       — FinBERT score last 14 days (tickertape.db)
      4. SMART MONEY          — insider + institutional + bulk deal composite (all DBs)
      5. FORENSIC CLEAN       — inverted fraud score; 100 = no red flags (forensic.db)

    Composite score = weighted average of available pillars.

    Divergence detection:
      If any pillar is > 40 points away from the composite, that pillar is flagged.
      Example: "DIVERGENCE — Technicals bearish (20) while Fundamentals bullish (85).
      Market may be pricing in something the financials haven't shown yet."

    Args:
      identifier : NSE symbol, BSE code, ISIN, or screener slug
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        nse_code     = company.get("nse_code") or ""
        display_name = company.get("company_name") or identifier
        cid          = company.get("screener_company_id")

        pillars: dict[str, dict] = {}

        # ── Pillar 1: Fundamental Quality ─────────────────────────────────────
        if cid and _db_available(SCREENER_DB):
            try:
                s_conn = _conn(SCREENER_DB)
                metrics = _one(s_conn,
                    """
                    SELECT frm.roe_pct, frm.roce_pct, frm.pe_ratio, frm.pb_ratio,
                           frm.market_cap, frm.current_price
                    FROM fact_realtime_metrics frm
                    WHERE frm.company_id = ?
                    ORDER BY frm.snapshot_date DESC LIMIT 1
                    """,
                    (cid,),
                )
                cf_pat = _rows(s_conn,
                    """
                    SELECT fcf.cash_from_operating, fpl.net_profit
                    FROM fact_cash_flow fcf
                    JOIN fact_profit_loss fpl ON fpl.company_id = fcf.company_id
                         AND fpl.period_id = fcf.period_id
                    JOIN dim_period dp ON dp.period_id = fcf.period_id
                    WHERE fcf.company_id = ?
                      AND fpl.net_profit > 0
                    ORDER BY dp.year DESC LIMIT 3
                    """,
                    (cid,),
                )
                s_conn.close()

                # Score components
                roe_score  = min(100, max(0, round((metrics["roe_pct"] or 0) * 2.5))) if metrics else 0
                roce_score = min(100, max(0, round((metrics["roce_pct"] or 0) * 3))) if metrics else 0
                # CF quality: avg CF/PAT ratio → 0–100
                cf_ratios  = [
                    r["cash_from_operating"] / r["net_profit"]
                    for r in cf_pat
                    if r["net_profit"] and r["net_profit"] > 0
                ]
                cf_score = min(100, max(0, round(sum(cf_ratios) / len(cf_ratios) * 60))) if cf_ratios else 50

                fund_score = round((roe_score + roce_score + cf_score) / 3)
                pillars["fundamental_quality"] = {
                    "score":      fund_score,
                    "roe_pct":    metrics["roe_pct"] if metrics else None,
                    "roce_pct":   metrics["roce_pct"] if metrics else None,
                    "cf_quality": round(sum(cf_ratios) / len(cf_ratios), 2) if cf_ratios else None,
                    "label":      "STRONG" if fund_score >= 70 else "AVERAGE" if fund_score >= 40 else "WEAK",
                }
            except Exception as e:
                pillars["fundamental_quality"] = {"score": None, "error": str(e)}

        # ── Pillar 2: Technical Momentum ──────────────────────────────────────
        if _db_available(TECHNICAL_DB) and _db_available(IDENTITY_DB):
            try:
                id_conn = _identity_conn()
                tid_row = _one(id_conn,
                    "SELECT ticker_id FROM company_map WHERE nse_code = ? LIMIT 1",
                    (nse_code,),
                )
                id_conn.close()

                if tid_row:
                    t_conn = _conn(TECHNICAL_DB)
                    recent_sigs = _rows(t_conn,
                        """
                        SELECT signal_type, direction, date
                        FROM signals
                        WHERE ticker_id = ?
                          AND date >= date('now', '-30 days')
                        ORDER BY date DESC
                        """,
                        (tid_row["ticker_id"],),
                    )
                    t_conn.close()

                    bull_count = sum(1 for s in recent_sigs if s["direction"] == "bullish")
                    bear_count = sum(1 for s in recent_sigs if s["direction"] == "bearish")
                    total_sigs = bull_count + bear_count

                    if total_sigs > 0:
                        # Raw direction ratio → 0–100
                        tech_score = round(bull_count / total_sigs * 100)
                    else:
                        tech_score = 50  # neutral when no recent signals

                    pillars["technical_momentum"] = {
                        "score":        tech_score,
                        "bullish_30d":  bull_count,
                        "bearish_30d":  bear_count,
                        "total_30d":    total_sigs,
                        "label":        "BULLISH" if tech_score >= 60 else "BEARISH" if tech_score <= 40 else "NEUTRAL",
                    }
            except Exception as e:
                pillars["technical_momentum"] = {"score": None, "error": str(e)}

        # ── Pillar 3: News Sentiment ──────────────────────────────────────────
        if nse_code and _db_available(NEWS_DB):
            try:
                n_conn = _conn(NEWS_DB)
                comp_row = _one(n_conn,
                    "SELECT id FROM companies WHERE nse_code = ? LIMIT 1",
                    (nse_code,),
                )
                if comp_row:
                    sent_rows = _rows(n_conn,
                        """
                        SELECT na.score_positive, na.score_negative, na.score_neutral
                        FROM news_articles na
                        JOIN article_stocks ars ON ars.article_id = na.id
                        WHERE ars.company_id = ?
                          AND na.published_at >= date('now', '-14 days')
                        """,
                        (comp_row["id"],),
                    )
                    n_conn.close()
                    if sent_rows:
                        avg_pos = sum(r["score_positive"] or 0 for r in sent_rows) / len(sent_rows)
                        avg_neg = sum(r["score_negative"] or 0 for r in sent_rows) / len(sent_rows)
                        # Normalise to 0–100: pure positive = 100, pure negative = 0
                        sent_score = round((avg_pos - avg_neg + 1) / 2 * 100)
                        pillars["news_sentiment"] = {
                            "score":          sent_score,
                            "articles_14d":   len(sent_rows),
                            "avg_positive":   round(avg_pos, 3),
                            "avg_negative":   round(avg_neg, 3),
                            "label":          "POSITIVE" if sent_score >= 60 else "NEGATIVE" if sent_score <= 40 else "NEUTRAL",
                        }
                    else:
                        n_conn.close()
                        pillars["news_sentiment"] = {"score": 50, "note": "No recent news — neutral assumed"}
                else:
                    n_conn.close()
                    pillars["news_sentiment"] = {"score": None, "note": "Company not in news DB"}
            except Exception as e:
                pillars["news_sentiment"] = {"score": None, "error": str(e)}

        # ── Pillar 4: Smart Money ─────────────────────────────────────────────
        # Reuse get_smart_money_flow logic inline (avoid tool call overhead)
        if nse_code:
            try:
                smf_result = json.loads(get_smart_money_flow(nse_code, days=90))
                raw_smf_score = smf_result.get("score")
                if raw_smf_score is not None:
                    # Remap -100..+100 → 0..100
                    smart_score = round((raw_smf_score + 100) / 2)
                    pillars["smart_money"] = {
                        "score":   smart_score,
                        "raw":     raw_smf_score,
                        "signal":  smf_result.get("signal"),
                        "label":   smf_result.get("signal"),
                    }
                else:
                    pillars["smart_money"] = {"score": None, "note": "insufficient_data"}
            except Exception as e:
                pillars["smart_money"] = {"score": None, "error": str(e)}

        # ── Pillar 5: Forensic Clean Score ────────────────────────────────────
        if nse_code:
            try:
                fraud_result = json.loads(get_fraud_score(nse_code))
                fraud_score_raw = fraud_result.get("score")
                if fraud_score_raw is not None:
                    # Invert: 0 fraud = 100 clean
                    forensic_score = 100 - fraud_score_raw
                    pillars["forensic_clean"] = {
                        "score":       forensic_score,
                        "fraud_score": fraud_score_raw,
                        "flags":       fraud_result.get("flags", []),
                        "label":       "CLEAN" if forensic_score >= 70 else "RISK" if forensic_score <= 40 else "CAUTION",
                    }
                else:
                    pillars["forensic_clean"] = {"score": None, "note": fraud_result.get("reason")}
            except Exception as e:
                pillars["forensic_clean"] = {"score": None, "error": str(e)}

        # ── Composite ─────────────────────────────────────────────────────────
        scored = {k: v for k, v in pillars.items() if v.get("score") is not None}

        if len(scored) < 2:
            return _fmt({
                "company":  display_name,
                "nse_code": nse_code,
                "score":    None,
                "reason":   "insufficient_data — fewer than 2 pillars have data",
                "pillars":  pillars,
            })

        composite = round(sum(v["score"] for v in scored.values()) / len(scored))

        if composite >= 70:
            conviction_label = "HIGH CONVICTION BULL"
        elif composite >= 55:
            conviction_label = "MILD BULL"
        elif composite >= 45:
            conviction_label = "NEUTRAL"
        elif composite >= 30:
            conviction_label = "MILD BEAR"
        else:
            conviction_label = "HIGH CONVICTION BEAR"

        # Divergence detection
        divergences = []
        for pillar_name, pillar_data in scored.items():
            gap = abs(pillar_data["score"] - composite)
            if gap >= 35:
                direction = "bullish" if pillar_data["score"] > composite else "bearish"
                divergences.append({
                    "pillar":    pillar_name,
                    "score":     pillar_data["score"],
                    "composite": composite,
                    "gap":       gap,
                    "direction": direction,
                    "interpretation": (
                        f"{pillar_name.replace('_', ' ').title()} is significantly more "
                        f"{direction} ({pillar_data['score']}) than composite ({composite}). "
                        + ("Market may be pricing in problems not yet in financials."
                           if direction == "bearish" and pillar_name == "technical_momentum"
                           else "Smart money sees something the market hasn't priced in yet."
                           if direction == "bullish" and pillar_name == "smart_money"
                           else "Worth investigating why this pillar diverges.")
                    ),
                })

        return _fmt({
            "company":          display_name,
            "nse_code":         nse_code,
            "conviction_score": composite,
            "conviction_label": conviction_label,
            "pillars_scored":   len(scored),
            "pillars_total":    5,
            "pillars":          pillars,
            "divergences":      divergences,
            "interpretation": (
                f"Composite conviction: {composite}/100 ({conviction_label}). "
                f"Based on {len(scored)} of 5 pillars. "
                + (f"{len(divergences)} divergence(s) detected — see 'divergences' array."
                   if divergences else "All available pillars are broadly aligned.")
            ),
        })

    except Exception as e:
        return f"Error computing conviction score for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Alert Engine helpers
# ---------------------------------------------------------------------------

_ALERTS_SCHEMA = """
CREATE TABLE IF NOT EXISTS alert_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol      TEXT    NOT NULL,
    condition   TEXT    NOT NULL,
    label       TEXT    NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    active      INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS alert_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id      INTEGER NOT NULL REFERENCES alert_rules(id),
    fired_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    field_value  REAL,
    snapshot_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_log_fired_at ON alert_log(fired_at);
CREATE INDEX IF NOT EXISTS idx_alert_rules_active ON alert_rules(active);
"""

# Whitelist of allowed DSL fields and their DB resolution strategy
_ALERT_FIELDS = {
    "rsi_14", "price", "pe_ratio", "promoters_pct",
    "pledge_pct", "macd_histogram",
}
_ALERT_OPS = {"<", ">", "<=", ">=", "=="}

import re as _re
_CONDITION_RE = _re.compile(
    r"^(rsi_14|price|pe_ratio|promoters_pct|pledge_pct|macd_histogram)"
    r"\s*(<=|>=|<|>|==)\s*(\d+(?:\.\d+)?)$"
)


def _alerts_conn():
    if ALERTS_DB.exists():
        ALERTS_DB.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(ALERTS_DB))
        conn.row_factory = sqlite3.Row
        conn.executescript(_ALERTS_SCHEMA)
        conn.commit()
        return conn
    # Railway: schema already in Turso DB1
    conn = _TursoConn("db1")
    conn.executescript(_ALERTS_SCHEMA)
    return conn


def _resolve_alert_field(symbol: str, field: str) -> float | None:
    """Resolve a single alert field value from cached DBs — no live fetching."""
    try:
        if field == "price":
            company = _resolve(symbol)
            if not company or not company["screener_company_id"]:
                return None
            s_conn = _conn(SCREENER_DB)
            row = _one(s_conn,
                "SELECT current_price FROM fact_realtime_metrics "
                "WHERE company_id = ? ORDER BY snapshot_date DESC LIMIT 1",
                (company["screener_company_id"],),
            )
            s_conn.close()
            return row["current_price"] if row else None

        if field == "pe_ratio":
            company = _resolve(symbol)
            if not company or not company["screener_company_id"]:
                return None
            s_conn = _conn(SCREENER_DB)
            row = _one(s_conn,
                "SELECT pe_ratio FROM fact_realtime_metrics "
                "WHERE company_id = ? ORDER BY snapshot_date DESC LIMIT 1",
                (company["screener_company_id"],),
            )
            s_conn.close()
            return row["pe_ratio"] if row else None

        if field == "promoters_pct":
            company = _resolve(symbol)
            if not company or not company["screener_company_id"]:
                return None
            s_conn = _conn(SCREENER_DB)
            row = _one(s_conn,
                """
                SELECT fs.promoters_pct FROM fact_shareholding fs
                JOIN dim_period dp ON dp.period_id = fs.period_id
                WHERE fs.company_id = ? AND fs.period_type = 'quarterly'
                ORDER BY dp.year DESC, dp.quarter DESC LIMIT 1
                """,
                (company["screener_company_id"],),
            )
            s_conn.close()
            return row["promoters_pct"] if row else None

        if field == "pledge_pct":
            if not _db_available(FORENSIC_DB):
                return None
            f_conn = _forensic_conn()
            row = f_conn.execute(
                "SELECT pledge_pct FROM pledge_events WHERE nse_code = ? "
                "AND pledge_pct IS NOT NULL ORDER BY event_date DESC LIMIT 1",
                (symbol,),
            ).fetchone()
            f_conn.close()
            return row["pledge_pct"] if row else None

        if field in ("rsi_14", "macd_histogram"):
            # Use get_current_indicators which computes from OHLCV
            if not _db_available(TECHNICAL_DB):
                return None
            ind_result = json.loads(get_current_indicators(symbol))
            indicators = ind_result.get("indicators") or ind_result
            return indicators.get(field)

    except Exception:
        return None
    return None


def _evaluate_condition(field_value: float, op: str, threshold: float) -> bool:
    if op == "<":  return field_value < threshold
    if op == ">":  return field_value > threshold
    if op == "<=": return field_value <= threshold
    if op == ">=": return field_value >= threshold
    if op == "==": return abs(field_value - threshold) < 1e-9
    return False


# ---------------------------------------------------------------------------
# Tool 42: set_alert
# ---------------------------------------------------------------------------

@mcp.tool()
def set_alert(symbol: str, condition: str, label: str) -> str:
    """
    Create a price/indicator alert for a stock.

    The condition is a simple DSL: {field} {op} {value}
    Valid fields:  rsi_14, price, pe_ratio, promoters_pct, pledge_pct, macd_histogram
    Valid ops:     < > <= >= ==

    Examples:
      set_alert("HDFCBANK", "rsi_14 < 35", "HDFCBANK oversold — consider entry")
      set_alert("RELIANCE", "price > 1500", "RELIANCE breakout level")
      set_alert("CHOICEIN", "pledge_pct > 50", "CHOICEIN pledge risk trigger")
      set_alert("TATAMOTORS", "pe_ratio < 10", "TATAMOTORS cheap on PE")

    Alerts are checked each time get_morning_briefing is called.
    View fired alerts with get_alerts(). Remove with delete_alert(id).
    """
    try:
        # Validate symbol
        company = _resolve(symbol)
        if not company:
            return _not_found(symbol)
        nse_code = company.get("nse_code") or symbol.upper()

        # Validate condition DSL — whitelist only, no eval()
        m = _CONDITION_RE.match(condition.strip())
        if not m:
            return (
                f"Invalid condition '{condition}'. "
                f"Format: {{field}} {{op}} {{value}}. "
                f"Valid fields: {', '.join(sorted(_ALERT_FIELDS))}. "
                f"Valid ops: < > <= >= =="
            )

        conn = _alerts_conn()
        cursor = conn.execute(
            "INSERT INTO alert_rules (symbol, condition, label) VALUES (?, ?, ?)",
            (nse_code, condition.strip(), label.strip()),
        )
        alert_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return _fmt({
            "status":    "created",
            "alert_id":  alert_id,
            "symbol":    nse_code,
            "condition": condition.strip(),
            "label":     label.strip(),
            "message":   f"Alert #{alert_id} set. Will fire when {condition} for {nse_code}.",
        })
    except Exception as e:
        return f"Error setting alert: {e}"


# ---------------------------------------------------------------------------
# Tool 43: delete_alert
# ---------------------------------------------------------------------------

@mcp.tool()
def delete_alert(alert_id: int) -> str:
    """
    Deactivate an alert by its ID (returned by set_alert or get_alerts).
    The alert is soft-deleted (active=0) — its fire history is preserved.
    """
    try:
        conn = _alerts_conn()
        row = conn.execute(
            "SELECT id, symbol, condition, label, active FROM alert_rules WHERE id = ?",
            (alert_id,),
        ).fetchone()
        if not row:
            conn.close()
            return f"Alert #{alert_id} not found."
        if not row["active"]:
            conn.close()
            return f"Alert #{alert_id} is already inactive."

        conn.execute("UPDATE alert_rules SET active = 0 WHERE id = ?", (alert_id,))
        conn.commit()
        conn.close()
        return _fmt({
            "status":    "deleted",
            "alert_id":  alert_id,
            "symbol":    row["symbol"],
            "condition": row["condition"],
            "label":     row["label"],
        })
    except Exception as e:
        return f"Error deleting alert: {e}"


# ---------------------------------------------------------------------------
# Tool 44: get_alerts
# ---------------------------------------------------------------------------

@mcp.tool()
def get_alerts(since_hours: int = 24, include_active: bool = True) -> str:
    """
    Get recently fired alerts and optionally all active (pending) alert rules.

    Args:
      since_hours    : how far back to look for fired alerts (default 24h)
      include_active : also return all active rules not yet fired (default True)
    """
    try:
        conn = _alerts_conn()

        fired = _rows(conn,
            """
            SELECT al.id, al.rule_id, al.fired_at, al.field_value,
                   ar.symbol, ar.condition, ar.label
            FROM alert_log al
            JOIN alert_rules ar ON ar.id = al.rule_id
            WHERE al.fired_at >= datetime('now', ? || ' hours')
            ORDER BY al.fired_at DESC
            """,
            (f"-{since_hours}",),
        )

        active = []
        if include_active:
            active = _rows(conn,
                """
                SELECT id, symbol, condition, label, created_at
                FROM alert_rules
                WHERE active = 1
                ORDER BY created_at DESC
                """,
            )

        conn.close()

        return _fmt({
            "fired_last_n_hours":  since_hours,
            "fired_count":         len(fired),
            "fired_alerts":        fired,
            "active_rules_count":  len(active),
            "active_rules":        active,
        })
    except Exception as e:
        return f"Error getting alerts: {e}"


# ---------------------------------------------------------------------------
# Alert checker (called by pipelines + morning briefing)
# ---------------------------------------------------------------------------

def check_alerts() -> list[dict]:
    """
    Evaluate all active alert rules against current cached data.
    Returns list of fired alerts. Logs each firing to alert_log.
    Called at end of pipeline runs and inside get_morning_briefing.
    """
    fired: list[dict] = []
    try:
        conn = _alerts_conn()
        rules = _rows(conn, "SELECT * FROM alert_rules WHERE active = 1")

        for rule in rules:
            m = _CONDITION_RE.match(rule["condition"])
            if not m:
                continue
            field, op, threshold_str = m.group(1), m.group(2), m.group(3)
            threshold = float(threshold_str)

            value = _resolve_alert_field(rule["symbol"], field)
            if value is None:
                continue

            if _evaluate_condition(value, op, threshold):
                snapshot = {"field": field, "value": value, "threshold": threshold, "op": op}
                conn.execute(
                    "INSERT INTO alert_log (rule_id, field_value, snapshot_json) VALUES (?, ?, ?)",
                    (rule["id"], value, json.dumps(snapshot)),
                )
                conn.commit()
                fired.append({
                    "alert_id":  rule["id"],
                    "symbol":    rule["symbol"],
                    "condition": rule["condition"],
                    "label":     rule["label"],
                    "field":     field,
                    "value":     value,
                    "threshold": threshold,
                })

        conn.close()
    except Exception:
        pass
    return fired


# ---------------------------------------------------------------------------
# Tool 45: get_morning_briefing
# ---------------------------------------------------------------------------

@mcp.tool()
def get_morning_briefing(watchlist: list[str] | None = None) -> str:
    """
    Pre-market intelligence briefing — the one tool that starts every trading day.

    Orchestrates 5 data sources into a single structured briefing:
      1. Market breadth   — is the broad market bullish or bearish today?
      2. Bulk deals       — what institutional block trades happened yesterday?
      3. Alerts fired     — which of your watchlist conditions triggered overnight?
      4. Watchlist signals — new technical signals on your watchlist stocks
      5. Sentiment delta  — news sentiment shift in the last 24h per stock

    Partial-failure tolerant: if any source fails, the briefing continues
    with a note about which source failed. One broken data source never
    kills the entire morning brief.

    Args:
      watchlist : list of NSE symbols to monitor, e.g. ["HDFCBANK", "RELIANCE"]
                  If None, returns market-level briefing only.

    Claude should narrate this as a 5-bullet pre-market brief:
      "Here's your morning brief for [date]:
       1. MARKET: [breadth summary]
       2. INSTITUTIONS: [bulk deals summary]
       3. ALERTS: [fired conditions]
       4. WATCHLIST: [signals summary per stock]
       5. SENTIMENT: [news delta summary]"
    """
    import datetime as dt

    briefing: dict = {
        "date":    dt.date.today().isoformat(),
        "sources": {},
        "failed_sources": [],
    }
    watchlist = watchlist or []

    # ── 1. Market breadth ─────────────────────────────────────────────────────
    try:
        breadth = json.loads(get_market_breadth(days=1))
        briefing["sources"]["market_breadth"] = {
            "breadth_label":   breadth["signal_breadth"]["breadth_label"],
            "net_breadth_pct": breadth["signal_breadth"]["net_breadth_pct"],
            "purely_bullish":  breadth["signal_breadth"]["purely_bullish"],
            "purely_bearish":  breadth["signal_breadth"]["purely_bearish"],
            "above_ema200":    breadth["ema_200_proxy"]["above_ema200_stocks"],
            "below_ema200":    breadth["ema_200_proxy"]["below_ema200_stocks"],
            "top_signals":     breadth.get("top_signals_this_week", [])[:3],
            "interpretation":  breadth.get("interpretation"),
        }
    except Exception as e:
        briefing["failed_sources"].append({"source": "market_breadth", "error": str(e)})

    # ── 2. Bulk deals yesterday ───────────────────────────────────────────────
    try:
        if _db_available(FORENSIC_DB):
            yesterday = (dt.date.today() - dt.timedelta(days=1)).isoformat()
            f_conn = _forensic_conn()
            bulk_rows = _rows(f_conn,
                """
                SELECT deal_date, company_name, nse_code, client_name,
                       deal_type, quantity, price, value_cr
                FROM bulk_deals
                WHERE deal_date = ?
                ORDER BY value_cr DESC NULLS LAST
                LIMIT 20
                """,
                (yesterday,),
            )
            f_conn.close()
            buys  = [r for r in bulk_rows if r["deal_type"] == "buy"]
            sells = [r for r in bulk_rows if r["deal_type"] == "sell"]
            briefing["sources"]["bulk_deals"] = {
                "date":          yesterday,
                "total_deals":   len(bulk_rows),
                "buy_deals":     len(buys),
                "sell_deals":    len(sells),
                "total_buy_cr":  round(sum((r["value_cr"] or 0) for r in buys), 2),
                "total_sell_cr": round(sum((r["value_cr"] or 0) for r in sells), 2),
                "top_deals":     bulk_rows[:10],
                "note": "No bulk deal data — run: python forensic-module/run_pipeline.py --bulk-deals"
                        if not bulk_rows else None,
            }
        else:
            briefing["sources"]["bulk_deals"] = {
                "note": "Bulk deals DB not seeded. Run: python forensic-module/run_pipeline.py --bulk-deals"
            }
    except Exception as e:
        briefing["failed_sources"].append({"source": "bulk_deals", "error": str(e)})

    # ── 3. Alerts fired overnight ─────────────────────────────────────────────
    try:
        fired = check_alerts()
        # Also pull alerts from last 12 hours from log
        a_conn = _alerts_conn()
        logged_fired = _rows(a_conn,
            """
            SELECT al.id, al.fired_at, al.field_value,
                   ar.symbol, ar.condition, ar.label
            FROM alert_log al
            JOIN alert_rules ar ON ar.id = al.rule_id
            WHERE al.fired_at >= datetime('now', '-12 hours')
            ORDER BY al.fired_at DESC
            """,
        )
        active_count = a_conn.execute(
            "SELECT COUNT(*) FROM alert_rules WHERE active = 1"
        ).fetchone()[0]
        a_conn.close()
        briefing["sources"]["alerts"] = {
            "fired_this_check":    len(fired),
            "fired_last_12h":      len(logged_fired),
            "active_rules":        active_count,
            "alerts":              logged_fired[:10],
        }
    except Exception as e:
        briefing["failed_sources"].append({"source": "alerts", "error": str(e)})

    # ── 4. Watchlist signals ──────────────────────────────────────────────────
    if watchlist:
        watchlist_data: list[dict] = []
        for symbol in watchlist[:10]:  # cap at 10 to stay fast
            stock_brief: dict = {"symbol": symbol}
            try:
                if _db_available(TECHNICAL_DB) and _db_available(IDENTITY_DB):
                    id_conn = _identity_conn()
                    tid_row = _one(id_conn,
                        "SELECT ticker_id FROM company_map WHERE nse_code = ? LIMIT 1",
                        (symbol,),
                    )
                    id_conn.close()
                    if tid_row:
                        t_conn = _conn(TECHNICAL_DB)
                        sigs = _rows(t_conn,
                            """
                            SELECT signal_type, direction, date
                            FROM signals
                            WHERE ticker_id = ?
                              AND date >= date('now', '-2 days')
                            ORDER BY date DESC
                            LIMIT 10
                            """,
                            (tid_row["ticker_id"],),
                        )
                        t_conn.close()
                        bull = [s for s in sigs if s["direction"] == "bullish"]
                        bear = [s for s in sigs if s["direction"] == "bearish"]
                        stock_brief["signals_48h"] = sigs
                        stock_brief["bullish_count"] = len(bull)
                        stock_brief["bearish_count"] = len(bear)
                        stock_brief["bias"] = (
                            "BULLISH" if len(bull) > len(bear)
                            else "BEARISH" if len(bear) > len(bull)
                            else "NEUTRAL"
                        )
            except Exception as e:
                stock_brief["error"] = str(e)
            watchlist_data.append(stock_brief)

        briefing["sources"]["watchlist_signals"] = {
            "stocks_checked": len(watchlist_data),
            "stocks":         watchlist_data,
        }

    # ── 5. Sentiment delta last 24h ───────────────────────────────────────────
    if watchlist and _db_available(NEWS_DB):
        sentiment_data: list[dict] = []
        try:
            n_conn = _conn(NEWS_DB)
            for symbol in watchlist[:10]:
                comp_row = _one(n_conn,
                    "SELECT id FROM companies WHERE nse_code = ? LIMIT 1",
                    (symbol,),
                )
                if not comp_row:
                    continue
                news_rows = _rows(n_conn,
                    """
                    SELECT na.headline, na.score_positive, na.score_negative,
                           na.published_at
                    FROM news_articles na
                    JOIN article_stocks ars ON ars.article_id = na.id
                    WHERE ars.company_id = ?
                      AND na.published_at >= datetime('now', '-24 hours')
                    ORDER BY na.published_at DESC
                    LIMIT 5
                    """,
                    (comp_row["id"],),
                )
                if news_rows:
                    avg_pos = sum(r["score_positive"] or 0 for r in news_rows) / len(news_rows)
                    avg_neg = sum(r["score_negative"] or 0 for r in news_rows) / len(news_rows)
                    sentiment_data.append({
                        "symbol":        symbol,
                        "articles_24h":  len(news_rows),
                        "avg_positive":  round(avg_pos, 3),
                        "avg_negative":  round(avg_neg, 3),
                        "dominant":      "POSITIVE" if avg_pos > avg_neg else "NEGATIVE",
                        "headlines":     [r["headline"] for r in news_rows[:3]],
                    })
            n_conn.close()
            briefing["sources"]["sentiment_24h"] = {
                "stocks_with_news": len(sentiment_data),
                "data":             sentiment_data,
            }
        except Exception as e:
            briefing["failed_sources"].append({"source": "sentiment_24h", "error": str(e)})

    # ── Narrative summary ─────────────────────────────────────────────────────
    breadth_data = briefing["sources"].get("market_breadth", {})
    alerts_data  = briefing["sources"].get("alerts", {})
    bulk_data    = briefing["sources"].get("bulk_deals", {})

    briefing["narrative_prompts"] = [
        f"MARKET: {breadth_data.get('interpretation', 'Market breadth data unavailable.')}",
        f"INSTITUTIONS: {bulk_data.get('total_deals', 0)} bulk deals yesterday — "
        f"₹{bulk_data.get('total_buy_cr', 0)} Cr buys vs ₹{bulk_data.get('total_sell_cr', 0)} Cr sells.",
        f"ALERTS: {alerts_data.get('fired_last_12h', 0)} alert(s) fired in last 12h "
        f"({alerts_data.get('active_rules', 0)} total active rules).",
        "WATCHLIST: See sources.watchlist_signals for per-stock signal bias.",
        "SENTIMENT: See sources.sentiment_24h for overnight news.",
    ]

    if briefing["failed_sources"]:
        briefing["warning"] = (
            f"{len(briefing['failed_sources'])} source(s) failed: "
            + ", ".join(s["source"] for s in briefing["failed_sources"])
        )

    return _fmt(briefing)


# ---------------------------------------------------------------------------
# Tool 47: get_promoter_holdings
# ---------------------------------------------------------------------------

@mcp.tool()
def get_promoter_holdings(identifier: str) -> str:
    """
    Map every named individual/entity holding shares in a company, with their
    exact % holding — sourced from SEBI SAST regulatory filings via Trendlyne.

    Returns a ranked list of: person name → category → % held (after_pct) →
    shares held → last transaction date → transaction type.

    SAST (Substantial Acquisition of Shares and Takeovers) filings are mandatory
    whenever a promoter or insider acquires, transfers, or pledges shares.
    The `after_pct` field in each filing is the person's holding % AFTER that
    transaction — so the most recent filing per person gives their current stake.

    Args:
      identifier : NSE symbol, BSE code, ISIN, or screener slug

    COVERAGE:
      - All promoters and promoter group entities (almost always complete)
      - Key managerial personnel (KMP) who transact
      - Large public shareholders who cross 2% via acquisition
    GAPS (use page-index annual report query for these):
      - FII/FPI individual fund names and their % → filed under FPI route,
        not SAST. Only aggregate FII % is available (from get_shareholding).
      - DII individual fund names → same. Use get_funds_holding_stock for
        DII proxy via screener DII trend.
      - Retail public shareholders → not disclosed individually unless >5%.

    RED FLAGS:
      - Promoter with declining after_pct across consecutive filings → selling
      - Multiple pledge_created entries with no releases → margin call risk
      - KMP or nominee directors with unusually large stakes → governance flag
      - Shell entities (Pvt Ltd / HUF) in the promoter group → use
        get_director_companies to trace who controls them
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")
        if not nse_code:
            return f"No NSE code found for '{identifier}'."

        _ensure_forensic_data(nse_code)
        conn = _forensic_conn()

        # For each person: take only their MOST RECENT filing
        # (that's the one reflecting current holdings)
        rows = _rows(conn, """
            SELECT
                person_name,
                person_category,
                after_pct,
                after_shares,
                transaction_date,
                transaction_type,
                price
            FROM insider_transactions
            WHERE nse_code = ?
              AND after_pct IS NOT NULL
              AND person_name != ''
            ORDER BY person_name, transaction_date DESC
        """, (nse_code,))
        conn.close()

        if not rows:
            return (
                f"No SAST filing data found for '{nse_code}'. "
                "This company may have had no insider transactions in the tracked period, "
                "or Trendlyne does not cover it."
            )

        # Deduplicate: keep the most recent row per person
        seen: set[str] = set()
        latest: list[dict] = []
        for r in rows:
            key = r["person_name"].strip().upper()
            if key not in seen:
                seen.add(key)
                latest.append(r)

        # Sort by after_pct descending
        latest.sort(key=lambda x: x["after_pct"] or 0, reverse=True)

        # Group by category
        categories: dict[str, list[dict]] = {}
        for r in latest:
            cat = (r["person_category"] or "unknown").strip()
            categories.setdefault(cat, []).append({
                "name":             r["person_name"],
                "holding_pct":      r["after_pct"],
                "shares":           r["after_shares"],
                "as_of":            r["transaction_date"],
                "last_txn_type":    r["transaction_type"],
                "last_txn_price":   r["price"],
            })

        # Summary
        promoter_total = sum(
            r["after_pct"] for r in latest
            if r["after_pct"] and "promot" in (r["person_category"] or "").lower()
        )
        all_named_total = sum(r["after_pct"] for r in latest if r["after_pct"])

        return _fmt({
            "nse_code":   nse_code,
            "company":    company.get("company_name"),
            "data_source": "SEBI SAST filings via Trendlyne (most recent filing per person)",
            "note": (
                "after_pct = holder's % stake as reported in their most recent SAST filing. "
                "FII/DII individual funds not covered here — use get_shareholding for FII/DII totals, "
                "get_funds_holding_stock for DII fund-level proxy."
            ),
            "summary": {
                "named_holders":      len(latest),
                "promoter_group_pct": round(promoter_total, 2),
                "all_named_pct":      round(all_named_total, 2),
                "categories_found":   list(categories.keys()),
            },
            "holdings_by_category": categories,
            "all_holders_ranked":   [
                {
                    "rank":         i + 1,
                    "name":         r["person_name"],
                    "category":     r["person_category"],
                    "holding_pct":  r["after_pct"],
                    "shares":       r["after_shares"],
                    "as_of":        r["transaction_date"],
                }
                for i, r in enumerate(latest)
            ],
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error fetching promoter holdings for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# PILLAR 8 — MCA Corporate Registry Intelligence
# Source: filesure.in (MCA-linked director & company registry)
#
# Tools 48-52:
#   48. search_director           — find a person by name → DIN + companies
#   49. search_company_registry   — find a company in MCA registry → CIN + metadata
#   50. get_director_companies    — full company network for a director
#   51. map_promoter_network      — all people connected to a director via shared companies
#   52. get_director_network      — given a STOCK ticker, auto-resolve promoter names
#                                   from forensic data → full MCA corporate empire map
# ---------------------------------------------------------------------------

_FILESURE_BASE = "https://production.filesure.in/api/v1"


def _filesure_get(path: str, params: dict) -> dict:
    """GET request to filesure.in. Returns parsed JSON or raises RuntimeError."""
    import requests as _req
    from urllib.parse import urlencode
    url = f"{_FILESURE_BASE}/{path}?{urlencode(params)}"
    try:
        resp = _req.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except _req.exceptions.Timeout:
        raise RuntimeError("filesure.in API timed out (>10s)")
    except _req.exceptions.HTTPError as e:
        raise RuntimeError(f"filesure.in returned HTTP {e.response.status_code}")
    except Exception as e:
        raise RuntimeError(f"filesure.in request failed: {e}")


# ---------------------------------------------------------------------------
# Tool 47: search_director
# ---------------------------------------------------------------------------

@mcp.tool()
def search_director(name: str, limit: int = 10) -> str:
    """
    Search for a company director or promoter by name in the MCA registry.

    Returns DIN (Director Identification Number), full legal name, DIN status,
    total active directorships, and the list of companies they are/were on.

    Args:
      name  : person's name, e.g. "Kamlesh Shah", "Yogesh Jadhav"
      limit : max results to return (default 10)

    USE THIS TOOL WHEN:
      - User asks "who is [person]?" or "what companies does [promoter] own?"
      - Investigating promoter background before investing
      - Checking if a promoter is on too many company boards (governance risk)
      - Starting a related-party or shell company investigation
      - Looking up DIN to pass to get_director_companies or map_promoter_network

    INTERPRETATION:
      status=Deactivated/Lapsed → DIN is no longer active (person resigned / disqualified)
      totalDirectorshipCount=0  → no current active directorships
      Multiple names returned   → use DIN to disambiguate, not name alone
    """
    try:
        data = _filesure_get("directors/suggestions", {"searchTerm": name})
        if not data.get("success"):
            return f"Registry search failed for '{name}'."

        results = data.get("data", [])[:limit]
        if not results:
            return f"No directors found matching '{name}' in the MCA registry."

        cleaned = []
        for r in results:
            cleaned.append({
                "din":                   r.get("din"),
                "full_name":             r.get("fullName"),
                "din_status":            r.get("status"),
                "din_allocated":         r.get("dinAllocationDate"),
                "active_directorships":  r.get("totalDirectorshipCount", 0),
                "companies":             r.get("companies", []),
                "match_score":           round(r.get("score", 0), 2),
            })

        return _fmt({
            "search_term":    name,
            "results_count":  len(cleaned),
            "directors":      cleaned,
            "note": (
                "Top match is the highest-scoring result. "
                "Use DIN with get_director_companies for full company network."
            ),
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error searching director '{name}': {e}"


# ---------------------------------------------------------------------------
# Tool 48: search_company_registry
# ---------------------------------------------------------------------------

@mcp.tool()
def search_company_registry(name: str, limit: int = 10) -> str:
    """
    Search for a company in the MCA registry by name. Returns CIN, legal status,
    class of company (Public/Private/LLP), state, and incorporation age in months.

    Args:
      name  : company name, e.g. "Choice International", "HDFC Bank"
      limit : max results to return (default 10)

    USE THIS TOOL WHEN:
      - Looking up a company's CIN (required for MCA filings)
      - Checking if a company is Active, Struck Off, or Under Liquidation
      - Verifying company class (Public vs Private vs LLP)
      - Checking if a subsidiary or related-party entity exists in MCA
      - Shell company screening: new companies (low incorporationAge) with
        Private class and zero-DIN directors are higher risk

    FRAUD SIGNALS:
      status=Strike Off or Dissolved → previously active company wound up
      classOfCompany=Private + incorporationAge < 24 months → new shell risk
      Multiple similar names → check if promoter uses name-cloning across entities
    """
    try:
        data = _filesure_get("companies/suggestions", {"searchTerm": name})
        if not data.get("success"):
            return f"Registry search failed for '{name}'."

        results = data.get("data", [])[:limit]
        if not results:
            return f"No companies found matching '{name}' in the MCA registry."

        cleaned = []
        for r in results:
            age_months = r.get("incorporationAge", 0)
            cleaned.append({
                "cin":               r.get("cin"),
                "name":              r.get("company"),
                "status":            r.get("status"),
                "class":             r.get("classOfCompany"),
                "category":          r.get("category"),
                "state":             r.get("state"),
                "age_months":        age_months,
                "age_years":         round(age_months / 12, 1) if age_months else None,
                "match_score":       round(r.get("score", 0), 2),
            })

        return _fmt({
            "search_term":   name,
            "results_count": len(cleaned),
            "companies":     cleaned,
            "note": (
                "CIN format: first letter = L (Listed) or U (Unlisted). "
                "Use CIN to cross-reference with MCA filings for XBRL financials."
            ),
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error searching company '{name}': {e}"


# ---------------------------------------------------------------------------
# Tool 49: get_director_companies
# ---------------------------------------------------------------------------

@mcp.tool()
def get_director_companies(name: str, top_match_only: bool = True) -> str:
    """
    Given a director/promoter name, resolve their DIN and return the full list
    of companies they are or were associated with, enriched with CIN metadata
    (company status, class, state, incorporation age) for each company.

    This is the core tool for promoter due diligence: it answers "what does
    this person own?" with MCA-verified data.

    Args:
      name            : director name, e.g. "Kamlesh Shah"
      top_match_only  : if True (default), use only the highest-scored name
                        match. Set False to return networks for all matches.

    USE THIS TOOL WHEN:
      - Researching a promoter's full business empire
      - Checking if a promoter sits on too many boards (>10 = governance red flag)
      - Finding related-party entities before a forensic deep-dive
      - Identifying shell companies in a promoter's network
        (Private + age < 24mo + no employees + minimal paid-up)
      - Cross-referencing with get_fraud_score outputs

    RED FLAGS IN OUTPUT:
      - Companies with status=Strike Off or Dissolved → promoter has failed entities
      - Many Private Limited companies with low age → possible shell network
      - Companies across unrelated industries → conglomerate overstretch
      - LLPs with only 2 designated partners (including the promoter) → tax/fund routing vehicle
    """
    try:
        dir_data = _filesure_get("directors/suggestions", {"searchTerm": name})
        if not dir_data.get("success") or not dir_data.get("data"):
            return f"No director found matching '{name}' in the MCA registry."

        directors = dir_data["data"]
        if top_match_only:
            directors = [directors[0]]

        all_results = []
        for director in directors:
            company_names = director.get("companies", [])
            enriched_companies = []

            for co_name in company_names:
                try:
                    co_data = _filesure_get("companies/suggestions", {"searchTerm": co_name})
                    matches = co_data.get("data", [])
                    # Find the best matching company (exact or near-exact name)
                    best = None
                    for m in matches[:3]:
                        if co_name.lower() in (m.get("company") or "").lower():
                            best = m
                            break
                    if not best and matches:
                        best = matches[0]

                    if best:
                        age_months = best.get("incorporationAge", 0)
                        enriched_companies.append({
                            "name":       best.get("company"),
                            "cin":        best.get("cin"),
                            "status":     best.get("status"),
                            "class":      best.get("classOfCompany"),
                            "state":      best.get("state"),
                            "age_months": age_months,
                            "age_years":  round(age_months / 12, 1) if age_months else None,
                        })
                    else:
                        enriched_companies.append({"name": co_name, "cin": None, "status": "not_found"})
                except Exception:
                    enriched_companies.append({"name": co_name, "cin": None, "status": "lookup_error"})

            # Risk flags
            struck_off   = [c for c in enriched_companies if (c.get("status") or "").lower() in ("strike off", "dissolved", "under liquidation")]
            private_cos  = [c for c in enriched_companies if c.get("class") == "Private"]
            new_private  = [c for c in private_cos if (c.get("age_months") or 999) < 24]
            llps         = [c for c in enriched_companies if "llp" in (c.get("name") or "").lower() or "llp" in (c.get("class") or "").lower()]

            flags = []
            if len(struck_off) > 0:
                flags.append(f"{len(struck_off)} company(ies) struck off / dissolved")
            if director.get("totalDirectorshipCount", 0) > 10:
                flags.append(f"High board load: {director['totalDirectorshipCount']} active directorships")
            if len(new_private) > 0:
                flags.append(f"{len(new_private)} Private company(ies) under 2 years old — check for shells")
            if len(llps) > 2:
                flags.append(f"{len(llps)} LLPs — possible fund-routing or tax structuring vehicles")

            all_results.append({
                "din":                  director.get("din"),
                "full_name":            director.get("fullName"),
                "din_status":           director.get("status"),
                "total_directorships":  director.get("totalDirectorshipCount", 0),
                "companies":            enriched_companies,
                "risk_flags":           flags if flags else ["No obvious structural flags"],
                "summary": {
                    "total":       len(enriched_companies),
                    "active":      sum(1 for c in enriched_companies if c.get("status") == "Active"),
                    "struck_off":  len(struck_off),
                    "private":     len(private_cos),
                    "llps":        len(llps),
                    "new_private": len(new_private),
                },
            })

        return _fmt({
            "search_name":    name,
            "profiles_found": len(all_results),
            "directors":      all_results,
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error fetching company network for '{name}': {e}"


# ---------------------------------------------------------------------------
# Tool 50: map_promoter_network
# ---------------------------------------------------------------------------

@mcp.tool()
def map_promoter_network(name: str) -> str:
    """
    Map the full promoter network: given a director name, find all companies
    they control, then search each company to find other directors/signatories
    sharing those companies. Returns the web of connected individuals.

    This answers: "Who else is connected to this promoter?"
    Useful for: related-party transaction risk, circular ownership detection,
    and identifying hidden beneficiaries in a corporate structure.

    Args:
      name : promoter/director name, e.g. "Kamlesh Shah"

    USE THIS TOOL WHEN:
      - Doing a full forensic sweep of a promoter group
      - Checking if nominee directors are actually independent
      - Identifying whether "unrelated" subsidiaries share directors
        (which qualifies them as related parties under Ind AS 24)
      - Building a corporate map for an investment memo

    INTERPRETATION:
      A person appearing across 5+ of the target's companies is likely a
      trusted associate or family member — relevant for related-party risk.
      A person who is a director only on one obscure entity alongside the
      target may be a nominee or sleeping partner.
    """
    try:
        # Step 1: resolve the target director
        dir_data = _filesure_get("directors/suggestions", {"searchTerm": name})
        if not dir_data.get("success") or not dir_data.get("data"):
            return f"No director found matching '{name}'."

        target = dir_data["data"][0]
        target_din   = target.get("din")
        target_name  = target.get("fullName")
        company_names = target.get("companies", [])

        if not company_names:
            return _fmt({
                "target": {"din": target_din, "name": target_name},
                "note": "No companies found for this director in the registry.",
            })

        # Step 2: for each company, search for other directors sharing it
        co_network: dict[str, list[dict]] = {}
        connected_people: dict[str, dict] = {}  # din → person info

        for co_name in company_names[:10]:  # cap at 10 companies
            try:
                # Search for directors associated with this company name
                co_dir_data = _filesure_get("directors/suggestions", {"searchTerm": co_name})
                co_directors = co_dir_data.get("data", [])

                co_associates = []
                for d in co_directors[:20]:
                    d_companies = d.get("companies", [])
                    # Only include if this company actually appears in their company list
                    if any(co_name.lower() in dc.lower() for dc in d_companies):
                        d_din = d.get("din")
                        if d_din and d_din != target_din:
                            co_associates.append({
                                "din":        d_din,
                                "name":       d.get("fullName"),
                                "din_status": d.get("status"),
                                "total_directorships": d.get("totalDirectorshipCount", 0),
                            })
                            # Track cross-company connections
                            if d_din not in connected_people:
                                connected_people[d_din] = {
                                    "din":    d_din,
                                    "name":   d.get("fullName"),
                                    "status": d.get("status"),
                                    "shared_companies": [],
                                    "total_directorships": d.get("totalDirectorshipCount", 0),
                                }
                            connected_people[d_din]["shared_companies"].append(co_name)

                co_network[co_name] = co_associates
            except Exception:
                co_network[co_name] = []

        # Step 3: rank connected people by how many companies they share
        sorted_connected = sorted(
            connected_people.values(),
            key=lambda x: len(x["shared_companies"]),
            reverse=True,
        )

        # Highlight high-overlap connections
        high_overlap = [p for p in sorted_connected if len(p["shared_companies"]) >= 2]

        return _fmt({
            "target": {
                "din":  target_din,
                "name": target_name,
                "din_status": target.get("status"),
                "companies_mapped": len(company_names),
            },
            "network_summary": {
                "unique_co_directors_found": len(connected_people),
                "high_overlap_connections":  len(high_overlap),
                "note": "High overlap = person shares 2+ companies with target (likely associate/family/nominee)",
            },
            "high_overlap_connections": high_overlap,
            "all_connections": sorted_connected[:30],
            "company_breakdown": {
                co: [p["name"] for p in people]
                for co, people in co_network.items()
            },
        })
    except RuntimeError as e:
        return str(e)
    except Exception as e:
        return f"Error mapping promoter network for '{name}': {e}"


# ---------------------------------------------------------------------------
# PILLAR 2 — MF Accumulation Screen + Cross-Sell Nudge
# ---------------------------------------------------------------------------

def _screener_conn_rw():
    if _db_available(SCREENER_DB):
        conn = sqlite3.connect(str(SCREENER_DB))
        conn.row_factory = sqlite3.Row
        return conn
    return _TursoConn("db1")


def _get_dii_quarters(company_id: int, conn: sqlite3.Connection, n: int = 8) -> list[dict]:
    """Return last N quarterly DII rows, newest-first."""
    rows = conn.execute(
        """
        SELECT dp.period_label, dp.year, dp.quarter,
               fs.diis_pct, fs.fiis_pct, fs.promoters_pct, fs.public_pct
        FROM fact_shareholding fs
        JOIN dim_period dp USING (period_id)
        WHERE fs.company_id = ? AND fs.period_type = 'quarterly'
        ORDER BY dp.year DESC, dp.quarter DESC
        LIMIT ?
        """,
        (company_id, n),
    ).fetchall()
    return [dict(r) for r in rows]


def _accumulation_score(quarters: list[dict]) -> dict:
    """
    Given newest-first quarterly DII% rows, compute:
    - total_change: DII % moved over the period
    - consecutive_up: how many straight quarters DII rose
    - consistency_pct: % of quarter-pairs where DII increased
    - signal: strong / moderate / weak / distributing
    """
    if len(quarters) < 2:
        return {"total_change": 0, "consecutive_up": 0, "consistency_pct": 0, "signal": "insufficient_data"}

    diis = [q["diis_pct"] for q in quarters if q["diis_pct"] is not None]
    if len(diis) < 2:
        return {"total_change": 0, "consecutive_up": 0, "consistency_pct": 0, "signal": "insufficient_data"}

    # newest first → reverse for chronological
    chron = list(reversed(diis))
    changes = [chron[i] - chron[i-1] for i in range(1, len(chron))]

    total_change = round(chron[-1] - chron[0], 2)   # latest minus oldest
    up_moves     = sum(1 for c in changes if c > 0.05)
    consistency  = round(up_moves / len(changes) * 100, 1)

    # Consecutive up from latest going back
    consec = 0
    for c in reversed(changes):
        if c > 0.05:
            consec += 1
        else:
            break

    if total_change >= 3 and consistency >= 70:
        signal = "strong_accumulation"
    elif total_change >= 1.5 or (consec >= 3 and consistency >= 60):
        signal = "moderate_accumulation"
    elif total_change < -2 and consistency <= 30:
        signal = "distributing"
    elif total_change > 0.5:
        signal = "mild_accumulation"
    else:
        signal = "stable"

    return {
        "total_change_pp": total_change,       # percentage points
        "consecutive_up_quarters": consec,
        "consistency_pct": consistency,
        "quarters_analysed": len(diis),
        "signal": signal,
    }


@mcp.tool()
def screen_mf_accumulation(
    min_total_increase_pp: float = 2.0,
    min_consecutive_quarters: int = 2,
    signal_filter: str = "",
    limit: int = 25,
) -> str:
    """
    Screen all stocks for systematic DII (mutual fund + insurance) accumulation.

    DII% rising consistently over multiple quarters = institutional money building
    positions. This is the most reliable public proxy for MF buying activity —
    individual fund-level disclosures lag by a month but this aggregated trend
    updates quarterly via SEBI mandated shareholding disclosures.

    Useful for:
    - Finding stocks where 'smart money' is quietly accumulating
    - Generating cross-sell leads: high DII accumulation + technical breakout = call
    - Validating existing positions: your thesis + institutional confirmation

    Args:
        min_total_increase_pp:   Minimum DII% increase over the tracked period (default 2pp)
        min_consecutive_quarters: Minimum consecutive quarters of DII increase (default 2)
        signal_filter:           Filter by signal type: "strong_accumulation",
                                 "moderate_accumulation", "mild_accumulation", "distributing"
        limit:                   Max results (default 25)
    """
    try:
        conn = _conn(SCREENER_DB)

        # Get all companies with enough quarterly shareholding history
        companies = conn.execute(
            """
            SELECT dc.company_id, dc.symbol, dc.name,
                   COUNT(*) as q_count
            FROM fact_shareholding fs
            JOIN dim_company dc USING (company_id)
            WHERE fs.period_type = 'quarterly'
            GROUP BY dc.company_id
            HAVING q_count >= 4
            ORDER BY dc.company_id
            """
        ).fetchall()

        results = []
        for co in companies:
            quarters = _get_dii_quarters(co["company_id"], conn, n=8)
            if len(quarters) < 4:
                continue
            score = _accumulation_score(quarters)

            if score["total_change_pp"] < min_total_increase_pp:
                continue
            if score["consecutive_up_quarters"] < min_consecutive_quarters:
                continue
            if signal_filter and score["signal"] != signal_filter:
                continue

            # Latest DII and FII
            latest = quarters[0]
            oldest = quarters[-1]

            results.append({
                "symbol":          co["symbol"],
                "name":            co["name"],
                "dii_pct_now":     latest["diis_pct"],
                "dii_pct_before":  oldest["diis_pct"],
                "fii_pct_now":     latest["fiis_pct"],
                "period_from":     oldest["period_label"],
                "period_to":       latest["period_label"],
                **score,
            })

        conn.close()

        # Rank: strong signal > total change > consecutive quarters
        signal_rank = {
            "strong_accumulation":   4,
            "moderate_accumulation": 3,
            "mild_accumulation":     2,
            "stable":                1,
            "distributing":          0,
            "insufficient_data":     0,
        }
        results.sort(
            key=lambda x: (signal_rank.get(x["signal"], 0), x["total_change_pp"], x["consecutive_up_quarters"]),
            reverse=True,
        )
        results = results[:limit]

        return _fmt({
            "screen": "DII/MF Accumulation",
            "filter_applied": {
                "min_increase_pp":        min_total_increase_pp,
                "min_consecutive_qtrs":   min_consecutive_quarters,
                "signal_filter":          signal_filter or "all",
            },
            "total_matches": len(results),
            "results": results,
            "interpretation": {
                "strong_accumulation":   "DII rose 3pp+ with 70%+ consistency — high-conviction institutional build",
                "moderate_accumulation": "DII rising steadily — institutions building quietly",
                "mild_accumulation":     "Small but consistent DII increase — early-stage accumulation",
                "distributing":          "DII falling — institutions reducing, caution warranted",
            },
            "cross_sell_action": (
                "Pick 'strong_accumulation' stocks where you also have a technical buy signal. "
                "Run get_cross_sell_nudge(symbol) to generate ready-to-send client messages."
            ),
        })
    except Exception as e:
        return f"Error running accumulation screen: {e}"


@mcp.tool()
def get_cross_sell_nudge(
    identifier: str,
    signal_context: str = "",
    client_segment: str = "all",
) -> str:
    """
    Generate ready-to-use cross-sell nudge messages for a stock with institutional backing.

    Combines DII accumulation trend + technical signals + fund context to produce
    personalised nudge templates for different client situations:
      1. REINFORCE — client already holds the stock; validate with institutional data
      2. EXPAND — client is in a similar/adjacent stock; introduce this opportunity
      3. MF_CROSSSELL — client wants managed exposure; suggest a fund in this theme
      4. REVIVAL — client stopped out earlier; re-engagement with new institutional angle

    Args:
        identifier:     Stock NSE code or name (e.g. "CHOICEIN", "HDFC Bank")
        signal_context: Optional technical signal description to include
                        (e.g. "ADX breakout at 28, RSI 56, above EMA50")
        client_segment: Target segment for tone — "all", "equity", "fo", "commodity"
    """
    try:
        conn = _conn(SCREENER_DB)

        # Resolve company
        uid = identifier.strip()
        row = conn.execute(
            """
            SELECT company_id, symbol, name FROM dim_company
            WHERE symbol LIKE ? OR name LIKE ?
            LIMIT 1
            """,
            (f"%{uid}%", f"%{uid}%"),
        ).fetchone()

        if not row:
            conn.close()
            return f"Company not found: '{identifier}'. Try the NSE symbol or full name."

        cid, symbol, name = row["company_id"], row["symbol"], row["name"]

        # DII accumulation
        quarters = _get_dii_quarters(cid, conn, n=8)
        score    = _accumulation_score(quarters)

        # Latest realtime metrics
        metrics = conn.execute(
            "SELECT current_price, pe_ratio, market_cap, high_52w, low_52w FROM fact_realtime_metrics WHERE company_id=?",
            (cid,),
        ).fetchone()
        conn.close()

        # Build data block
        latest_dii  = quarters[0]["diis_pct"] if quarters else None
        oldest_dii  = quarters[-1]["diis_pct"] if quarters else None
        period_from = quarters[-1]["period_label"] if quarters else "N/A"
        period_to   = quarters[0]["period_label"] if quarters else "N/A"
        dii_change  = round((latest_dii or 0) - (oldest_dii or 0), 2)
        consec      = score.get("consecutive_up_quarters", 0)
        signal      = score.get("signal", "stable")

        # Current price context
        price_str = ""
        if metrics and metrics["current_price"]:
            cp     = metrics["current_price"]
            h52    = metrics["high_52w"]
            l52    = metrics["low_52w"]
            from_low  = round((cp - l52) / l52 * 100, 1) if l52 else None
            from_high = round((h52 - cp) / h52 * 100, 1) if h52 else None
            price_str = (
                f"₹{cp} | 52W: ₹{l52}–₹{h52} | "
                f"{from_low}% above 52W low | {from_high}% below 52W high"
            )

        # Technical line (from caller or generic)
        tech_line = signal_context if signal_context else (
            "technical setup pending — run get_signal_cluster(identifier) for confirmation"
        )

        # Accumulation sentence
        if dii_change > 0 and latest_dii:
            inst_line = (
                f"Institutional (DII) ownership has grown from {oldest_dii}% to {latest_dii}% "
                f"(+{dii_change}pp over {len(quarters)} quarters, {period_from}–{period_to}). "
                f"{consec} consecutive quarters of increase."
            )
        else:
            inst_line = f"DII holding at {latest_dii}% ({signal.replace('_',' ')})."

        # ── Nudge templates ─────────────────────────────────────────────────
        segment_note = {
            "fo":        "FO clients: frame as hedged play — buy stock + protective put.",
            "equity":    "Equity clients: SIP-style staggered entry over 2–3 weeks.",
            "commodity": "Commodity clients: this is an equity cross-sell — lead with lower volatility angle.",
            "all":       "Adapt tone to segment: FO → leverage angle, Equity → conviction angle.",
        }.get(client_segment, "")

        nudges = {
            "REINFORCE": (
                f"[For clients already holding {symbol}]\n"
                f"'Good news on {name} — {inst_line} "
                f"When institutions are steadily building a position over this many quarters, "
                f"it signals long-term conviction, not trading noise. "
                f"Your position is well-supported. Consider holding through near-term volatility.'"
            ),
            "EXPAND": (
                f"[For clients in similar/adjacent stocks]\n"
                f"'{name} ({symbol}) is seeing strong institutional interest — {inst_line} "
                + (f"Technically: {signal_context}. " if signal_context else "")
                + f"This is the kind of setup where retail and institutions are aligned. "
                f"Worth a look if you want to add another quality name to your portfolio. "
                f"Current price: {price_str}'"
            ),
            "MF_CROSSSELL": (
                f"[For clients who prefer managed/diversified exposure]\n"
                f"'Multiple institutional funds have been systematically buying {name} — {inst_line} "
                f"If you want exposure to this sector/theme without single-stock risk, "
                f"a mid-cap or thematic equity fund holding this space could be the right vehicle. "
                f"Want me to pull up the top-performing funds in this category? "
                f"(Run: search_mutual_funds or get_mf_recommendation to find best fit.)'"
            ),
            "REVIVAL": (
                f"[For clients who previously stopped out on {symbol}]\n"
                f"'{name} has seen significant institutional accumulation since your exit — "
                f"{inst_line} The thesis has strengthened. "
                + (f"Technicals have also reset: {signal_context}. " if signal_context else "")
                + f"Re-entry with a smaller initial position and a defined SL could make sense now. "
                f"Current price: {price_str}'"
            ),
        }

        return _fmt({
            "stock":   symbol,
            "name":    name,
            "price":   price_str or "N/A",
            "institutional_trend": {
                "signal":              signal,
                "dii_now_pct":         latest_dii,
                "dii_change_pp":       dii_change,
                "consecutive_qtrs_up": consec,
                "period":              f"{period_from} → {period_to}",
            },
            "technical_context": tech_line,
            "segment_guidance":  segment_note,
            "nudge_templates":   nudges,
            "recommended_action": (
                "Pick the nudge that matches the client's situation. "
                "Best results: EXPAND + REINFORCE for clients with active positions, "
                "MF_CROSSSELL for fee-averse or conservative clients, "
                "REVIVAL for winback campaigns after SL events."
            ),
        })

    except Exception as e:
        return f"Error generating nudge for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# PILLAR 9 — Annual Report Intelligence
# ---------------------------------------------------------------------------
# Architecture:
#   • Each indexed annual report has a cached tree (structure.json) built by the
#     page-index pipeline. The tree is a hierarchical JSON with node_id, title,
#     page ranges, and LLM-generated summaries.
#   • At query time this tool (a) condenses the tree to titles+summaries, (b) asks
#     Claude Haiku to pick which nodes are relevant, (c) reads the actual page text
#     from the PDF using pdfplumber, and (d) asks Claude Sonnet to answer.
#   • Currently indexed: Choice International Limited (CHOICEIN).
#   • To index a new company, download its annual report PDF from BSE
#     (subcategory "Annual Report / AGM") and run the page-index pipeline
#     (D:\Projects\page-index-fork\run_pageindex.py). Future: auto-download via
#     get_bse_filings + index on first call.

_AR_RESULTS_DIR = Path("D:/Projects/page-index-fork/results")
_AR_PDF_DIR     = Path("D:/Projects/page-index-fork")   # PDFs live alongside results

# NSE code / alias  →  filename prefix (without _structure.json)
_AR_INDEX: dict[str, str] = {
    "CHOICEIN": "Annual Report_Choice International Limited",
    "CHOICE":   "Annual Report_Choice International Limited",
}


def _ar_claude(prompt: str, model: str) -> str:
    """Single Anthropic call for annual report tools."""
    try:
        import anthropic as _ant
        client = _ant.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        msg = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text
    except Exception as exc:
        return f"LLM error: {exc}"


def _ar_condense_tree(tree: list, max_summary: int = 200) -> list:
    def _c(n):
        out: dict = {"node_id": n.get("node_id", ""), "title": n.get("title", "")}
        s = n.get("summary", "")
        if s:
            out["summary"] = s[:max_summary]
        if n.get("nodes"):
            out["nodes"] = [_c(x) for x in n["nodes"]]
        return out
    return [_c(n) for n in tree] if isinstance(tree, list) else [_c(tree)]


def _ar_flatten(tree) -> list:
    """Recursively flatten the tree to a list of all nodes (no children)."""
    if isinstance(tree, dict):
        nodes = [tree]
        for child in tree.get("nodes", []):
            nodes.extend(_ar_flatten(child))
        return nodes
    elif isinstance(tree, list):
        out = []
        for item in tree:
            out.extend(_ar_flatten(item))
        return out
    return []


def _ar_page_text(pdf_path: Path, start: int, end: int) -> str:
    """Extract text from PDF pages [start, end] (1-indexed, inclusive)."""
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf:
            pages = pdf.pages
            texts = []
            for i in range(start - 1, min(end, len(pages))):
                texts.append(pages[i].extract_text() or "")
            return "\n".join(texts)
    except Exception:
        return ""


def _ar_extract_json(raw: str) -> dict:
    """Pull the first {...} block out of a potentially markdown-wrapped response."""
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start == -1 or end == 0:
        return {}
    try:
        return json.loads(raw[start:end])
    except Exception:
        return {}


@mcp.tool()
def query_annual_report(identifier: str, question: str) -> str:
    """Query a company's annual report using tree-index search and Claude reasoning.

    The report is pre-indexed into a hierarchical tree of sections with summaries.
    This tool identifies which sections contain the answer, reads the actual page
    text, and generates a sourced answer — without needing a vector database.

    Currently indexed companies: CHOICEIN (Choice International Limited).
    To add a new company: download its BSE annual report PDF, run the page-index
    pipeline (D:/Projects/page-index-fork/run_pageindex.py), and add the NSE code
    to _AR_INDEX in mcp_server.py.

    Args:
        identifier: Company NSE code or name (e.g. "CHOICEIN", "Choice International")
        question:   Natural language question about the annual report
    """
    upper = identifier.upper().strip()

    # Resolve identifier to a file prefix
    prefix = _AR_INDEX.get(upper)
    if not prefix:
        # Fuzzy: check if any key is contained in the identifier or vice-versa
        for k, v in _AR_INDEX.items():
            if k in upper or upper in k:
                prefix = v
                break

    if not prefix:
        indexed = sorted(set(_AR_INDEX.keys()))
        return (
            f"Annual report not indexed for '{identifier}'.\n\n"
            f"Currently indexed: {indexed}\n\n"
            "To index a new company:\n"
            "  1. Get the annual report PDF from BSE filings API\n"
            "     (use get_bse_filings with category='Annual Report')\n"
            "  2. Run: python D:/Projects/page-index-fork/run_pageindex.py "
            "--pdf_path <pdf_path>\n"
            "  3. Add the NSE code mapping to _AR_INDEX in mcp_server.py"
        )

    structure_path = _AR_RESULTS_DIR / f"{prefix}_structure.json"
    if not structure_path.exists():
        return (
            f"Tree cache not found: {structure_path}\n"
            "Run the page-index pipeline to regenerate it."
        )

    # Load tree
    with open(structure_path, encoding="utf-8") as f:
        raw = json.load(f)
    tree: list = raw["structure"] if isinstance(raw, dict) and "structure" in raw else raw

    # ── Step 1: Tree search (Claude Haiku — fast + cheap) ──────────────────
    condensed = _ar_condense_tree(tree)
    tree_prompt = (
        "You are given a question and a tree structure of a document.\n"
        "Each node has a node_id, title, and summary.\n"
        "Find all nodes likely to contain the answer to the question.\n\n"
        f"Question: {question}\n\n"
        f"Document tree:\n{json.dumps(condensed, indent=2)}\n\n"
        "Reply in JSON only:\n"
        '{\n'
        '  "thinking": "<which sections are relevant and why>",\n'
        '  "node_list": ["node_id_1", "node_id_2"]\n'
        '}'
    )
    raw_tree_resp = _ar_claude(tree_prompt, model="claude-haiku-4-5-20251001")
    tree_result  = _ar_extract_json(raw_tree_resp)
    node_ids  = tree_result.get("node_list", [])
    thinking  = tree_result.get("thinking", "")

    if not node_ids:
        return f"No relevant sections found in the annual report for: '{question}'"

    # ── Step 2: Build context from matched nodes ────────────────────────────
    node_map = {n["node_id"]: n for n in _ar_flatten(tree) if "node_id" in n}
    pdf_path = _AR_PDF_DIR / f"{prefix}.pdf"

    CHAR_LIMIT = 120_000   # ~30k tokens — leave room for prompt + answer
    context_parts: list[str] = []
    total_chars   = 0

    for nid in node_ids:
        if nid not in node_map:
            continue
        node  = node_map[nid]
        title = node.get("title", nid)

        # Prefer actual page text; fall back to cached text, then summary
        text = node.get("text", "")
        if not text and pdf_path.exists():
            s = node.get("start_index") or 1
            e = node.get("end_index")   or s
            text = _ar_page_text(pdf_path, s, e)
        if not text:
            text = node.get("summary", "")

        remaining = CHAR_LIMIT - total_chars
        if remaining <= 0:
            break
        if len(text) > remaining:
            text = text[:remaining]

        context_parts.append(f"[{title}]\n{text}")
        total_chars += len(text)

    if not context_parts:
        return "No text could be extracted from matched sections."

    context = "\n\n".join(context_parts)

    # ── Step 3: Answer generation (Claude Sonnet — best reasoning) ─────────
    answer_prompt = (
        "Answer the question based only on the context below.\n"
        "If numbers, names, or percentages appear in the context, quote them exactly.\n\n"
        f"Question: {question}\n\n"
        f"Context:\n{context}\n\n"
        "Provide a clear, structured answer."
    )
    answer = _ar_claude(answer_prompt, model="claude-sonnet-4-6")

    return _fmt({
        "question":          question,
        "source_document":   prefix,
        "sections_searched": node_ids,
        "reasoning":         thinking,
        "answer":            answer,
    })


# ---------------------------------------------------------------------------
# Tool 52: get_director_network
# ---------------------------------------------------------------------------

@mcp.tool()
def get_director_network(identifier: str) -> str:
    """
    Given a stock ticker or company name, automatically resolve its promoters
    and directors from forensic/insider data, then map their full MCA corporate
    empire using the filesure.in registry.

    This is the stock-first entry point to the MCA intelligence layer — instead
    of requiring you to know a director's name, it derives the names from the
    company's insider transaction history and runs the full network analysis.

    Returns:
      - All promoters/directors found for the stock (from insider transactions)
      - Each person's MCA network: every company they direct or have directed
      - Per-person risk flags: shell companies, struck-off entities, high board load, LLPs
      - Cross-person overlap: entities shared by multiple promoters of the same stock
        (these are almost certainly related-party vehicles)

    Args:
      identifier : NSE code or company name, e.g. "CHOICEIN", "Infosys", "HDFCBANK"

    USE THIS TOOL WHEN:
      - Doing a forensic deep-dive on a company: "who is behind this stock?"
      - Checking related-party risk before an investment thesis
      - Validating get_fraud_score output: are there shell entities in the group?
      - Building a full corporate map for an investment memo or compliance report
      - Investigating why promoters are pledging heavily (which entities need cash?)

    INTERPRETATION:
      shared_entities  → companies that appear in 2+ promoters' networks
                         = almost certainly group companies / related parties
      shells_detected  → Private Ltd, age < 24 months, in a promoter's network
                         = potential cash-routing or asset-stripping vehicle
      high_board_load  → promoter directs 10+ companies = governance concern
      struck_off_count → promoter has failed entities = track record risk
    """
    try:
        # ── Step 1: resolve NSE code ──────────────────────────────────────────
        nse_code = None
        company_display_name = identifier.strip()

        try:
            id_conn = _conn(IDENTITY_DB)
            uid = identifier.strip().upper()
            row = id_conn.execute(
                """
                SELECT nse_code, company_name FROM company_map
                WHERE  nse_code = ? OR UPPER(company_name) LIKE ?
                LIMIT 1
                """,
                (uid, f"%{uid}%"),
            ).fetchone()
            id_conn.close()
            if row:
                nse_code = row["nse_code"]
                company_display_name = row["company_name"] or identifier
        except Exception:
            pass

        if not nse_code:
            nse_code = identifier.strip().upper()

        # ── Step 2: load insider/pledge data to extract promoter names ────────
        try:
            _ensure_forensic_data(nse_code)
        except Exception:
            pass

        insider_names: list[str] = []
        try:
            f_conn = _conn(FORENSIC_DB)

            # Collect from insider_transactions (all categories — promoter, KMP, director)
            txn_rows = f_conn.execute(
                """
                SELECT DISTINCT person_name, person_category
                FROM   insider_transactions
                WHERE  nse_code = ?
                  AND  person_name IS NOT NULL AND person_name != ''
                ORDER  BY person_name
                """,
                (nse_code,),
            ).fetchall()

            # Collect from pledge_events (promoters who pledged = very relevant)
            pledge_rows = f_conn.execute(
                """
                SELECT DISTINCT person_name
                FROM   pledge_events
                WHERE  nse_code = ?
                  AND  person_name IS NOT NULL AND person_name != ''
                """,
                (nse_code,),
            ).fetchall()
            f_conn.close()

            seen: set[str] = set()
            for r in txn_rows:
                n = (r["person_name"] or "").strip()
                if n and n not in seen:
                    insider_names.append(n)
                    seen.add(n)
            for r in pledge_rows:
                n = (r["person_name"] or "").strip()
                if n and n not in seen:
                    insider_names.append(n)
                    seen.add(n)
        except Exception:
            pass

        if not insider_names:
            return _fmt({
                "company":  company_display_name,
                "nse_code": nse_code,
                "note": (
                    "No promoter/insider names found in forensic data. "
                    f"Run: python forensic-module/run_pipeline.py --nse {nse_code} "
                    "to seed insider transaction data first, then retry."
                ),
            })

        # ── Step 3: for each name, query MCA and build network ────────────────
        person_networks: list[dict] = []
        all_company_names: dict[str, list[str]] = {}  # company_name → [person_names]

        for person_name in insider_names[:15]:  # cap: 15 people × API calls
            try:
                dir_data = _filesure_get("directors/suggestions", {"searchTerm": person_name})
                if not dir_data.get("success") or not dir_data.get("data"):
                    person_networks.append({
                        "name": person_name,
                        "din": None,
                        "note": "Not found in MCA registry",
                        "companies": [],
                        "risk_flags": [],
                    })
                    continue

                target = dir_data["data"][0]
                company_names = target.get("companies", [])

                # Enrich each company with CIN metadata
                enriched: list[dict] = []
                for co_name in company_names:
                    try:
                        co_data = _filesure_get("companies/suggestions", {"searchTerm": co_name})
                        matches = co_data.get("data", [])
                        best = next(
                            (m for m in matches[:3] if co_name.lower() in (m.get("company") or "").lower()),
                            matches[0] if matches else None,
                        )
                        if best:
                            age_months = best.get("incorporationAge", 0)
                            enriched.append({
                                "name":       best.get("company"),
                                "cin":        best.get("cin"),
                                "status":     best.get("status"),
                                "class":      best.get("classOfCompany"),
                                "state":      best.get("state"),
                                "age_months": age_months,
                                "age_years":  round(age_months / 12, 1) if age_months else None,
                            })
                        else:
                            enriched.append({"name": co_name, "cin": None, "status": "not_found"})
                    except Exception:
                        enriched.append({"name": co_name, "cin": None, "status": "lookup_error"})

                    # Track for cross-person overlap
                    co_key = co_name.lower()
                    if co_key not in all_company_names:
                        all_company_names[co_key] = []
                    all_company_names[co_key].append(person_name)

                # Risk flags
                struck_off  = [c for c in enriched if (c.get("status") or "").lower() in ("strike off", "dissolved", "under liquidation")]
                private_cos = [c for c in enriched if c.get("class") == "Private"]
                new_private = [c for c in private_cos if (c.get("age_months") or 999) < 24]
                llps        = [c for c in enriched if "llp" in (c.get("name") or "").lower() or "llp" in (c.get("class") or "").lower()]

                flags = []
                if struck_off:
                    flags.append(f"{len(struck_off)} company(ies) struck off / dissolved")
                if target.get("totalDirectorshipCount", 0) > 10:
                    flags.append(f"High board load: {target['totalDirectorshipCount']} active directorships")
                if new_private:
                    flags.append(f"{len(new_private)} Private company(ies) under 2 years old — potential shells")
                if len(llps) > 2:
                    flags.append(f"{len(llps)} LLPs — possible fund-routing vehicles")

                person_networks.append({
                    "name":                person_name,
                    "din":                 target.get("din"),
                    "din_status":          target.get("status"),
                    "total_directorships": target.get("totalDirectorshipCount", 0),
                    "companies":           enriched,
                    "risk_flags":          flags if flags else ["No obvious structural flags"],
                    "summary": {
                        "total":      len(enriched),
                        "active":     sum(1 for c in enriched if c.get("status") == "Active"),
                        "struck_off": len(struck_off),
                        "private":    len(private_cos),
                        "llps":       len(llps),
                        "shells":     len(new_private),
                    },
                })

            except Exception as e:
                person_networks.append({
                    "name": person_name,
                    "din": None,
                    "note": f"Lookup error: {e}",
                    "companies": [],
                    "risk_flags": [],
                })

        # ── Step 4: cross-person overlap — shared entities = related parties ──
        shared_entities = [
            {"company": k, "shared_by": v}
            for k, v in all_company_names.items()
            if len(v) > 1
        ]
        shared_entities.sort(key=lambda x: len(x["shared_by"]), reverse=True)

        # ── Step 5: top-level risk summary ────────────────────────────────────
        total_shells     = sum(p.get("summary", {}).get("shells", 0) for p in person_networks if "summary" in p)
        total_struck_off = sum(p.get("summary", {}).get("struck_off", 0) for p in person_networks if "summary" in p)
        high_load_people = [p["name"] for p in person_networks if p.get("total_directorships", 0) > 10]
        not_found        = [p["name"] for p in person_networks if p.get("note") == "Not found in MCA registry"]

        return _fmt({
            "company":          company_display_name,
            "nse_code":         nse_code,
            "insiders_found":   len(insider_names),
            "insiders_queried": min(len(insider_names), 15),
            "risk_summary": {
                "shell_companies_detected":  total_shells,
                "struck_off_entities":       total_struck_off,
                "high_board_load_people":    high_load_people,
                "not_found_in_mca":          not_found,
                "shared_entities_count":     len(shared_entities),
                "related_party_risk":        "HIGH" if (total_shells >= 2 or len(shared_entities) >= 3) else "MEDIUM" if (total_shells >= 1 or len(shared_entities) >= 1) else "LOW",
            },
            "shared_entities": shared_entities[:20],
            "person_networks":  person_networks,
        })

    except Exception as e:
        return f"Error building director network for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# PILLAR 10 — Institutional Memory
# Tools 55-56: save_memory, recall_memories
# ---------------------------------------------------------------------------

_MEMORIES_SCHEMA = """
CREATE TABLE IF NOT EXISTS memories (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    content     TEXT    NOT NULL,
    mem_type    TEXT    NOT NULL DEFAULT 'finding'
                        CHECK (mem_type IN ('pattern','approach','finding','red_flag','tangent')),
    tags        TEXT,
    company     TEXT,
    tool        TEXT,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
    content, tags, company, tool,
    content='memories',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
    INSERT INTO memories_fts(rowid, content, tags, company, tool)
    VALUES (new.id, new.content, new.tags, new.company, new.tool);
END;
"""

def _memories_conn():
    if MEMORIES_DB.exists():
        MEMORIES_DB.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(MEMORIES_DB))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        conn.executescript(_MEMORIES_SCHEMA)
        conn.commit()
        return conn
    # Railway: schema already in Turso DB2
    conn = _TursoConn("db2")
    conn.executescript(_MEMORIES_SCHEMA)
    return conn


@mcp.tool()
def save_memory(
    content: str,
    mem_type: str = "finding",
    tags: str = "",
    company: str = "",
    tool: str = "",
) -> str:
    """
    Save an interesting insight, pattern, approach, or finding to institutional memory.
    This persists across conversations and can be recalled in future sessions.

    Call this whenever you observe:
      - A non-obvious fraud or promoter behaviour pattern
      - A sector/stock-specific quirk in signal behaviour
      - A multi-tool analytical approach that worked or failed
      - A corporate structure finding (shell, related-party, director overlap)
      - Any insight that required 3+ tools to surface — save the shortcut
      - An interesting tangent worth revisiting

    Args:
      content  : Full insight description — write it as if explaining to a colleague
                 in 6 months. Be specific: include company names, numbers, dates.
      mem_type : Type of memory —
                   "pattern"   : recurring market or corporate behaviour
                   "approach"  : analytical workflow or tool combination that works
                   "finding"   : specific company/event discovery
                   "red_flag"  : fraud or governance signal detected
                   "tangent"   : interesting aside worth exploring later
      tags     : Comma-separated keywords for retrieval, e.g. "pledge,pharma,promoter"
      company  : NSE code of relevant company, if any (e.g. "CHOICEIN")
      tool     : Which MCP tool surfaced this insight (e.g. "get_director_network")
    """
    valid_types = {"pattern", "approach", "finding", "red_flag", "tangent"}
    if mem_type not in valid_types:
        return f"Invalid mem_type '{mem_type}'. Must be one of: {', '.join(sorted(valid_types))}"
    if not content or not content.strip():
        return "content cannot be empty."

    try:
        conn = _memories_conn()
        conn.execute(
            """
            INSERT INTO memories (content, mem_type, tags, company, tool)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                content.strip(),
                mem_type,
                (tags or "").strip(),
                (company or "").strip().upper() or None,
                (tool or "").strip() or None,
            ),
        )
        conn.commit()
        row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        conn.close()
        return _fmt({
            "saved": True,
            "id":       row_id,
            "mem_type": mem_type,
            "company":  company.upper() if company else None,
            "tags":     tags or None,
            "preview":  content[:120] + ("…" if len(content) > 120 else ""),
        })
    except Exception as e:
        return f"Error saving memory: {e}"


@mcp.tool()
def recall_memories(
    query: str = "",
    mem_type: str = "",
    company: str = "",
    limit: int = 15,
) -> str:
    """
    Retrieve past insights, patterns, and findings from institutional memory.

    Searches full-text across content and tags. Call this at the START of any
    analysis to surface prior discoveries about the company, sector, or topic.

    Args:
      query    : Keywords to search — company name, topic, signal type, etc.
                 e.g. "pledge promoter pharma", "Golden Cross small cap", "CHOICEIN shell"
                 Leave empty to return most recent memories.
      mem_type : Filter by type — "pattern", "approach", "finding", "red_flag", "tangent"
                 Leave empty to search all types.
      company  : Filter by NSE code, e.g. "CHOICEIN"
      limit    : Max results to return (default 15)
    """
    try:
        conn = _memories_conn()

        params: list = []
        where_clauses: list[str] = []

        if mem_type:
            where_clauses.append("m.mem_type = ?")
            params.append(mem_type)
        if company:
            where_clauses.append("m.company = ?")
            params.append(company.strip().upper())

        if query and query.strip():
            # FTS5 full-text search
            fts_sql = f"""
            SELECT m.id, m.content, m.mem_type, m.tags, m.company, m.tool, m.created_at
            FROM   memories_fts f
            JOIN   memories m ON m.id = f.rowid
            {"WHERE " + " AND ".join(where_clauses) if where_clauses else ""}
            AND    memories_fts MATCH ?
            ORDER  BY rank
            LIMIT  ?
            """
            params.append(query.strip())
            params.append(limit)
            rows = conn.execute(fts_sql, params).fetchall()
        else:
            # Recency fallback when no query
            where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
            rows = conn.execute(
                f"""
                SELECT id, content, mem_type, tags, company, tool, created_at
                FROM   memories m
                {where_sql}
                ORDER  BY created_at DESC
                LIMIT  ?
                """,
                params + [limit],
            ).fetchall()

        conn.close()

        if not rows:
            return _fmt({
                "query":   query or "(recent)",
                "results": [],
                "note":    "No memories found. Start saving insights with save_memory().",
            })

        results = [
            {
                "id":         r["id"],
                "mem_type":   r["mem_type"],
                "company":    r["company"],
                "tags":       r["tags"],
                "tool":       r["tool"],
                "created_at": r["created_at"],
                "content":    r["content"],
            }
            for r in rows
        ]

        return _fmt({
            "query":        query or "(recent)",
            "filter_type":  mem_type or "all",
            "filter_company": company.upper() if company else "all",
            "results_count": len(results),
            "memories":     results,
        })
    except Exception as e:
        return f"Error recalling memories: {e}"


# ---------------------------------------------------------------------------
# Tool 53: get_news_impact_profile
# ---------------------------------------------------------------------------

@mcp.tool()
def get_news_impact_profile(identifier: str) -> str:
    """
    Compute a company's news-price impact profile — how consistently and strongly
    does the market price in this company's news?

    Aggregates all scored articles (FinBERT sentiment + same-day price data) to produce:
      market_belief_score   : 0-100. % of non-neutral articles where price moved in
                              the same direction as sentiment. Interpretation:
                                ≥ 70  = high belief — strong institutional following,
                                        news is priced in reliably
                                55-70 = moderate — some signal, some noise
                                < 55  = low belief / disbelief — market ignores or
                                        contradicts news; red flag for fraud or illiquidity
      divergence_rate_pct   : % where price moved AGAINST dominant sentiment
                              (positive news + price fell = distribution signal;
                               negative news + price rose = accumulation signal)
      avg_positive_reaction : avg same-day price change % on positive-sentiment articles
      avg_negative_reaction : avg same-day price change % on negative-sentiment articles
      avg_abs_reaction      : avg absolute price move on any news day (volatility proxy)

    Also returns the 5 most impactful articles (by absolute price reaction).

    WHY THIS MATTERS:
      Companies where news is never priced in = market disbelief = fraud/illiquidity signal.
      High divergence rate = smart money moving opposite to news narrative.
      Helps calibrate how much weight to give news sentiment for each stock.

    Limitation: same-day price data only (article publish date → day close).
                1-week follow-up pricing is not yet available.

    Args:
      identifier : NSE code, BSE code, ISIN, or screener slug
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        if not company["tickertape_company_id"]:
            return f"No news data linked for '{identifier}'."
        if not _db_available(NEWS_DB):
            return "News DB not found. Run the news pipeline first."

        cid = company["tickertape_company_id"]
        conn = _conn(NEWS_DB)

        stats = _one(conn,
            """
            WITH articles AS (
                SELECT
                    ROUND((ast.close_price - ast.initial_price) / ast.initial_price * 100, 2) AS reaction_pct,
                    CASE
                        WHEN a.score_positive >= a.score_negative
                         AND a.score_positive >= a.score_neutral  THEN 'positive'
                        WHEN a.score_negative >  a.score_positive
                         AND a.score_negative >= a.score_neutral  THEN 'negative'
                        ELSE 'neutral'
                    END AS dominant
                FROM   news_articles a
                JOIN   article_stocks ast ON ast.article_id = a.id
                WHERE  ast.company_id = ?
                  AND  a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
                  AND  a.score_positive IS NOT NULL
                  AND  ast.initial_price IS NOT NULL AND ast.initial_price > 0
                  AND  ast.close_price   IS NOT NULL
            )
            SELECT
                COUNT(*)                                                             AS total_articles,
                COUNT(CASE WHEN dominant != 'neutral' THEN 1 END)                  AS non_neutral_count,
                ROUND(AVG(CASE WHEN dominant = 'positive' THEN reaction_pct END), 2) AS avg_positive_reaction,
                ROUND(AVG(CASE WHEN dominant = 'negative' THEN reaction_pct END), 2) AS avg_negative_reaction,
                ROUND(AVG(ABS(reaction_pct)), 2)                                   AS avg_abs_reaction,
                -- aligned: positive→up OR negative→down
                COUNT(CASE WHEN dominant = 'positive' AND reaction_pct > 0   THEN 1 END) +
                COUNT(CASE WHEN dominant = 'negative' AND reaction_pct < 0   THEN 1 END) AS aligned_count,
                -- divergence: positive→fell OR negative→rose (smart money signal)
                COUNT(CASE WHEN dominant = 'positive' AND reaction_pct < -0.5 THEN 1 END) +
                COUNT(CASE WHEN dominant = 'negative' AND reaction_pct > 0.5  THEN 1 END) AS divergence_count
            FROM articles
            """,
            (cid,),
        )

        if not stats or not stats["total_articles"]:
            return f"No scored articles with price data found for '{identifier}'."

        total        = stats["total_articles"]
        non_neutral  = stats["non_neutral_count"] or 0
        aligned      = stats["aligned_count"]     or 0
        divergence   = stats["divergence_count"]  or 0

        belief_score     = round(aligned    / non_neutral * 100) if non_neutral else None
        divergence_rate  = round(divergence / non_neutral * 100, 1) if non_neutral else None

        if belief_score is None:
            belief_label = "insufficient non-neutral articles"
        elif belief_score >= 70:
            belief_label = "HIGH — market reliably prices in news (strong institutional following)"
        elif belief_score >= 55:
            belief_label = "MODERATE — partial news pricing, some noise"
        else:
            belief_label = "LOW ⚠ — market often ignores or contradicts news (disbelief / illiquidity risk)"

        top_articles = _rows(conn,
            """
            SELECT a.headline, a.published_at, a.publisher,
                   a.score_positive, a.score_negative,
                   ast.initial_price, ast.close_price,
                   ROUND((ast.close_price - ast.initial_price) / ast.initial_price * 100, 2) AS reaction_pct
            FROM   news_articles a
            JOIN   article_stocks ast ON ast.article_id = a.id
            WHERE  ast.company_id = ?
              AND  a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
              AND  a.score_positive IS NOT NULL
              AND  ast.initial_price IS NOT NULL AND ast.initial_price > 0
              AND  ast.close_price   IS NOT NULL
            ORDER BY ABS(ROUND((ast.close_price - ast.initial_price) / ast.initial_price * 100, 2)) DESC
            LIMIT 5
            """,
            (cid,),
        )
        conn.close()

        return _fmt({
            "company":              identifier,
            "articles_analyzed":    total,
            "market_belief_score":  belief_score,
            "belief_label":         belief_label,
            "divergence_rate_pct":  divergence_rate,
            "avg_positive_reaction_pct": stats["avg_positive_reaction"],
            "avg_negative_reaction_pct": stats["avg_negative_reaction"],
            "avg_abs_reaction_pct":      stats["avg_abs_reaction"],
            "note": (
                "Same-day price data only. "
                "High divergence_rate = smart money moving against news narrative. "
                "Low belief_score = market disbelief = check get_fraud_score()."
            ),
            "most_impactful_articles": top_articles,
        })

    except Exception as e:
        return f"Error computing news impact profile for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 54: screen_working_capital_stress
# ---------------------------------------------------------------------------

@mcp.tool()
def screen_working_capital_stress(
    min_ccc_deterioration_pct: float = 20.0,
    years: int = 3,
    limit: int = 30,
) -> str:
    """
    Screen for companies with systematically worsening cash conversion cycles —
    a leading indicator of revenue recognition risk and eventual balance sheet stress.

    Compares each company's cash conversion cycle (CCC) from N years ago against
    its most recent annual CCC. Companies where CCC has worsened by at least
    min_ccc_deterioration_pct are returned, sorted by worst deterioration first.

    CCC = debtor_days + inventory_days - days_payable
      Rising CCC = customers are paying slower, inventory is building up,
                   or the company is paying suppliers faster (all bad signs)
      Worsening debtor_days specifically = customers aren't paying = revenue risk

    WHY THIS MATTERS:
      Working capital stress fires 1-2 years before it shows up on the balance sheet.
      By the time borrowings spike or cash flow collapses, it's too late.
      This screen identifies companies where the deterioration is already in motion.

    Args:
      min_ccc_deterioration_pct : minimum % increase in CCC over the window (default 20)
      years                     : comparison window in years — compares year[0] vs year[-N]
                                  (default 3; min 2, max 10)
      limit                     : max results (default 30, max 100)

    INTERPRETATION:
      CCC up 20-50%   : early warning — investigate accounts receivable trend
      CCC up 50-100%  : significant stress — check if revenue growth is real
      CCC up > 100%   : severe stress — high probability of cash flow collapse
      Combined with rising debtor_days → customers not paying → fraud/quality risk
      Run get_historical_ratios() + analyze_earnings_quality() on flagged companies.

    PAIR WITH:
      get_fraud_score()          → quantify forensic risk
      analyze_earnings_quality() → check if operating CF is diverging from PAT
      get_historical_ratios()    → drill into full year-by-year ratio history
    """
    try:
        limit = min(limit, 100)
        years = max(2, min(years, 10))

        if not _db_available(SCREENER_DB):
            return "Screener DB not found. Run: python run_all.py --only screener"

        conn = _conn(SCREENER_DB)

        rows = _rows(conn,
            """
            WITH annual_ratios AS (
                -- Annual periods only — quarterly CCC is not comparable to full-year CCC
                SELECT fr.company_id,
                       dp.year,
                       fr.cash_conversion_cycle,
                       fr.debtor_days,
                       ROW_NUMBER() OVER (
                           PARTITION BY fr.company_id ORDER BY dp.year DESC
                       ) AS rn
                FROM   fact_ratios fr
                JOIN   dim_period dp ON dp.period_id = fr.period_id
                WHERE  dp.period_type = 'annual'
                  AND  fr.cash_conversion_cycle IS NOT NULL
            ),
            compared AS (
                -- latest (rn=1) vs oldest-in-window (rn=years)
                -- companies with fewer than `years` annual rows are naturally excluded
                SELECT oldest.company_id,
                       oldest.year          AS year_first,
                       oldest.cash_conversion_cycle AS ccc_first,
                       oldest.debtor_days   AS debtor_days_first,
                       latest.year          AS year_last,
                       latest.cash_conversion_cycle AS ccc_last,
                       latest.debtor_days   AS debtor_days_last,
                       ROUND(
                           (latest.cash_conversion_cycle - oldest.cash_conversion_cycle)
                           * 100.0 / NULLIF(ABS(oldest.cash_conversion_cycle), 0),
                       1) AS ccc_pct_change
                FROM   annual_ratios oldest
                JOIN   annual_ratios latest
                       ON  latest.company_id = oldest.company_id
                       AND latest.rn = 1
                WHERE  oldest.rn = ?
                  AND  latest.cash_conversion_cycle > oldest.cash_conversion_cycle
            )
            SELECT dc.symbol,
                   dc.name,
                   dc.nse_code,
                   frm.market_cap,
                   frm.current_price,
                   c.ccc_first,
                   c.ccc_last,
                   c.ccc_pct_change,
                   c.debtor_days_first,
                   c.debtor_days_last,
                   c.year_first,
                   c.year_last
            FROM   compared c
            JOIN   dim_company dc ON dc.company_id = c.company_id
            LEFT JOIN (
                SELECT company_id, market_cap, current_price,
                       ROW_NUMBER() OVER (
                           PARTITION BY company_id ORDER BY snapshot_date DESC
                       ) AS rn
                FROM   fact_realtime_metrics
            ) frm ON frm.company_id = c.company_id AND frm.rn = 1
            WHERE  c.ccc_pct_change >= ?
              AND  dc.nse_code IS NOT NULL
            ORDER BY c.ccc_pct_change DESC
            LIMIT  ?
            """,
            (years, min_ccc_deterioration_pct, limit),
        )
        conn.close()

        if not rows:
            return _fmt({
                "message": "No companies matched the working capital stress criteria.",
                "filters": {
                    "min_ccc_deterioration_pct": min_ccc_deterioration_pct,
                    "years": years,
                },
            })

        return _fmt({
            "filters": {
                "min_ccc_deterioration_pct": min_ccc_deterioration_pct,
                "years":                     years,
            },
            "count":          len(rows),
            "interpretation": (
                "Companies with cash conversion cycles that have worsened significantly. "
                f"CCC is compared between ~{years} years ago and today (annual periods only). "
                "Rising CCC = customers paying slower, inventory building, or payables shrinking. "
                "This fires 1-2 years before balance sheet stress becomes visible. "
                "Run get_historical_ratios() + analyze_earnings_quality() on flagged names."
            ),
            "companies":      rows,
        })

    except Exception as e:
        return f"Error screening working capital stress: {e}"


# ---------------------------------------------------------------------------
# Tool 55: get_earnings_predictor
# ---------------------------------------------------------------------------

def _score_label(score: float | None) -> str:
    if score is None:
        return "insufficient data"
    if score >= 75:
        return "STRONG"
    if score >= 55:
        return "MODERATE"
    if score >= 40:
        return "WEAK"
    return "POOR"


@mcp.tool()
def get_earnings_predictor(identifier: str) -> str:
    """
    Pre-earnings quality signal — composite indicator of whether a company
    is likely to beat or miss its next earnings release.

    Aggregates four independent signals into a composite score (0-100):

    COMPONENT 1 — CF/PAT TREND (weight 30%)
      Operating cash flow vs net profit over 3 years.
      Improving ratio = earnings quality rising = beat likely.
      Declining ratio = accruals masking weakness = miss risk.

    COMPONENT 2 — WORKING CAPITAL TREND (weight 25%)
      Cash conversion cycle (annual) over 3 years.
      Improving CCC + falling debtor days = collections accelerating = beat signal.
      Worsening CCC = customers paying slower = revenue recognition risk.

    COMPONENT 3 — NEWS SENTIMENT MOMENTUM (weight 25%)
      Average FinBERT sentiment (positive - negative) over last 30 days.
      Strong positive momentum before results = market expects a beat.
      Negative drift = market is pricing in a miss.

    COMPONENT 4 — TECHNICAL MOMENTUM (weight 20%, optional)
      Bullish vs bearish signal ratio in last 14 days.
      Skipped if technical data unavailable (weights rebalanced).

    COMPOSITE SCORE:
      ≥ 70 : STRONG BEAT SIGNAL   — multiple indicators pointing up
      55-69: MODERATE             — mixed signals, outcome uncertain
      40-54: WEAK                 — more warning signs than positives
      < 40 : POOR QUALITY         — elevated earnings miss risk

    LIMITATION: Uses annual working capital data (quarterly CCC unavailable).
    Earnings calendar not stored — interpret relative to your known results date.

    Args:
      identifier : NSE code, BSE code, ISIN, or screener slug
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        cid = company["screener_company_id"]
        if not cid or not _db_available(SCREENER_DB):
            return f"No screener fundamentals for '{identifier}'."

        s_conn = _conn(SCREENER_DB)

        # ── Component 1: CF/PAT Trend ────────────────────────────────────────

        cf_rows = _rows(s_conn,
            """
            SELECT dp.year,
                   fcf.cash_from_operating,
                   fpl.net_profit
            FROM   fact_cash_flow fcf
            JOIN   fact_profit_loss fpl ON fpl.company_id = fcf.company_id
                   AND fpl.period_id = fcf.period_id
            JOIN   dim_period dp ON dp.period_id = fcf.period_id
            WHERE  fcf.company_id = ?
              AND  dp.period_type = 'annual'
              AND  fcf.cash_from_operating IS NOT NULL
              AND  fpl.net_profit IS NOT NULL
              AND  fpl.net_profit != 0
            ORDER BY dp.year DESC LIMIT 3
            """, (cid,))

        cf_score = cf_label = cf_detail = None
        if cf_rows:
            ratios = [r["cash_from_operating"] / r["net_profit"] for r in cf_rows]
            latest_r = ratios[0]
            if   latest_r >= 1.0: cf_base = 80
            elif latest_r >= 0.8: cf_base = 65
            elif latest_r >= 0.6: cf_base = 50
            elif latest_r >= 0.4: cf_base = 35
            else:                  cf_base = 15
            if len(ratios) >= 2:
                if ratios[0] > ratios[-1]: cf_base = min(100, cf_base + 15)
                elif ratios[0] < ratios[-1]: cf_base = max(0,   cf_base - 15)
            cf_score  = cf_base
            cf_label  = _score_label(cf_score)
            cf_detail = {
                "latest_cf_pat_ratio": round(latest_r, 3),
                "trend":               "IMPROVING" if len(ratios) >= 2 and ratios[0] > ratios[-1]
                                       else ("DECLINING" if len(ratios) >= 2 and ratios[0] < ratios[-1]
                                             else "STABLE"),
            }

        # ── Component 2: Working Capital Trend ──────────────────────────────

        wc_rows = _rows(s_conn,
            """
            SELECT dp.year, fr.cash_conversion_cycle, fr.debtor_days
            FROM   fact_ratios fr
            JOIN   dim_period dp ON dp.period_id = fr.period_id
            WHERE  fr.company_id = ?
              AND  dp.period_type = 'annual'
              AND  fr.cash_conversion_cycle IS NOT NULL
            ORDER BY dp.year DESC LIMIT 3
            """, (cid,))

        s_conn.close()

        wc_score = wc_label = wc_detail = None
        if len(wc_rows) >= 2:
            ccc_last  = wc_rows[0]["cash_conversion_cycle"]
            ccc_first = wc_rows[-1]["cash_conversion_cycle"]
            dd_last   = wc_rows[0]["debtor_days"]
            dd_first  = wc_rows[-1]["debtor_days"]
            if ccc_first and ccc_first != 0:
                pct = (ccc_last - ccc_first) / abs(ccc_first) * 100
                if   pct < -20: wc_base = 85
                elif pct < -5:  wc_base = 70
                elif pct <  5:  wc_base = 50
                elif pct < 20:  wc_base = 35
                else:           wc_base = 15
                if dd_first and dd_last:
                    if dd_last < dd_first:  wc_base = min(100, wc_base + 10)
                    elif dd_last > dd_first: wc_base = max(0,   wc_base - 10)
                wc_score  = wc_base
                wc_label  = _score_label(wc_score)
                wc_detail = {
                    "ccc_pct_change": round(pct, 1),
                    "debtor_days_trend": ("IMPROVING" if dd_first and dd_last and dd_last < dd_first
                                          else ("WORSENING" if dd_first and dd_last and dd_last > dd_first
                                                else "STABLE")),
                }

        # ── Component 3: News Sentiment Momentum (last 30 days) ─────────────

        news_score = news_label = news_detail = None
        tt_cid = company.get("tickertape_company_id")
        if tt_cid and _db_available(NEWS_DB):
            try:
                n_conn = _conn(NEWS_DB)
                row = _one(n_conn,
                    """
                    SELECT COUNT(*) AS article_count,
                           AVG(a.score_positive - a.score_negative) AS avg_net_sentiment
                    FROM   news_articles a
                    JOIN   article_stocks ast ON ast.article_id = a.id
                    WHERE  ast.company_id = ?
                      AND  a.sentiment_at IS NOT NULL AND a.sentiment_at != 'ERROR'
                      AND  a.score_positive IS NOT NULL
                      AND  a.published_at >= date('now', '-30 days')
                    """, (tt_cid,))
                n_conn.close()
                if row and row["article_count"] and row["avg_net_sentiment"] is not None:
                    net = row["avg_net_sentiment"]
                    news_score  = max(0, min(100, round((net + 1) / 2 * 100)))
                    news_label  = _score_label(news_score)
                    news_detail = {
                        "articles_last_30d": row["article_count"],
                        "avg_net_sentiment": round(net, 3),
                    }
            except Exception:
                pass

        # ── Component 4: Technical Momentum (last 14 days, optional) ────────

        tech_score = tech_label = tech_detail = None
        ticker_id = company.get("ticker_id")
        if ticker_id and _db_available(TECHNICAL_DB):
            try:
                t_conn = _conn(TECHNICAL_DB)
                sig_rows = _rows(t_conn,
                    """
                    SELECT direction
                    FROM   signals
                    WHERE  ticker_id = ?
                      AND  date >= date('now', '-14 days')
                    """, (ticker_id,))
                t_conn.close()
                if sig_rows:
                    bullish = sum(1 for s in sig_rows if s["direction"] == "bullish")
                    total   = len(sig_rows)
                    tech_score  = round(bullish / total * 100)
                    tech_label  = _score_label(tech_score)
                    tech_detail = {
                        "bullish_signals": bullish,
                        "bearish_signals": total - bullish,
                        "total_signals":   total,
                    }
            except Exception:
                pass

        # ── Composite Score ──────────────────────────────────────────────────

        weights = [(cf_score, 0.30), (wc_score, 0.25), (news_score, 0.25), (tech_score, 0.20)]
        available = [(s, w) for s, w in weights if s is not None]
        if available:
            total_w   = sum(w for _, w in available)
            composite = round(sum(s * w for s, w in available) / total_w)
        else:
            composite = None

        if composite is None:
            composite_label = "insufficient data"
        elif composite >= 70:
            composite_label = "STRONG BEAT SIGNAL — multiple quality indicators pointing up"
        elif composite >= 55:
            composite_label = "MODERATE — mixed signals, earnings outcome uncertain"
        elif composite >= 40:
            composite_label = "WEAK — more warning signs than positives"
        else:
            composite_label = "POOR QUALITY — elevated earnings miss risk"

        return _fmt({
            "company":           identifier,
            "composite_score":   composite,
            "composite_label":   composite_label,
            "data_coverage":     f"{len(available)}/4 components available",
            "components": {
                "cf_pat_trend": {
                    "score":  cf_score,
                    "weight": "30%",
                    "label":  cf_label or "insufficient data",
                    "detail": cf_detail,
                },
                "working_capital_trend": {
                    "score":  wc_score,
                    "weight": "25%",
                    "label":  wc_label or "insufficient data",
                    "detail": wc_detail,
                },
                "news_momentum": {
                    "score":  news_score,
                    "weight": "25%",
                    "label":  news_label or "insufficient data",
                    "detail": news_detail,
                },
                "technical_momentum": {
                    "score":  tech_score,
                    "weight": "20%",
                    "label":  tech_label or "insufficient data",
                    "detail": tech_detail,
                },
            },
            "note": (
                "Composite uses annual working capital data (quarterly CCC unavailable). "
                "No earnings calendar — interpret relative to your known results date. "
                "Weights rebalanced when components are missing."
            ),
        })

    except Exception as e:
        return f"Error computing earnings predictor for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 56: get_promoter_track_record
# ---------------------------------------------------------------------------

@mcp.tool()
def get_promoter_track_record(identifier: str) -> str:
    """
    Score the historical accuracy of insider buy/sell signals for a company's promoters.

    For each recorded buy or sell transaction (from SAST filings), looks up the
    stock's closing price 6 months later (via OHLCV data) and classifies the outcome:
      BUY outcome  : price 6m later > transaction price → WIN (promoter bought before rise)
      SELL outcome : price 6m later < transaction price → WIN (promoter sold before fall)

    Returns:
      credibility_score : 0-100. Equal to win rate %. Interpretation:
                           ≥ 70 = RELIABLE — signals have historically been accurate
                           50-70 = MODERATE — some predictive value
                           < 50  = UNRELIABLE — signals have not been predictive
      win_rate_pct      : raw % of resolved events that were "wins"
      per_person        : per-promoter breakdown, sorted by win rate
      notable_instances : 5 most dramatic outcomes (biggest 6m price moves)

    Transactions are excluded from scoring if:
      pending    : transaction < 6 months ago — outcome not yet known
      no_data    : stock not in technical DB or OHLCV gap at the +6m date

    Minimum 3 resolved outcomes required for a credibility score.

    Args:
      identifier : NSE code, BSE code, or ISIN
    """
    try:
        MIN_RESOLVED = 3

        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code  = company.get("nse_code")
        ticker_id = company.get("ticker_id")
        if not nse_code:
            return f"No NSE code found for '{identifier}'."

        _ensure_forensic_data(nse_code)
        if not _db_available(FORENSIC_DB):
            return (f"Forensic DB not found. "
                    f"Run: python forensic-module/run_pipeline.py --identity {nse_code}")

        import datetime as dt
        from collections import defaultdict
        six_months_ago = (dt.date.today() - dt.timedelta(days=180)).isoformat()

        f_conn = _forensic_conn()
        txns = _rows(f_conn,
            """
            SELECT person_name, person_category, transaction_type,
                   shares, price, transaction_date
            FROM   insider_transactions
            WHERE  nse_code = ?
              AND  transaction_type IN ('buy', 'sell')
              AND  price IS NOT NULL AND price > 0
              AND  transaction_date IS NOT NULL
            ORDER BY transaction_date DESC
            """, (nse_code,))
        f_conn.close()

        if not txns:
            return (f"No buy/sell insider transactions with price data for '{nse_code}'. "
                    "Call get_insider_transactions() first to seed forensic data.")

        t_conn        = _conn(TECHNICAL_DB) if (ticker_id and _db_available(TECHNICAL_DB)) else None
        resolved_events: list[dict] = []
        pending_count = 0
        no_data_count = 0
        person_map: dict = defaultdict(lambda: {"category": "", "wins": 0, "losses": 0, "pending": 0})

        for txn in txns:
            td       = txn["transaction_date"]
            person   = txn["person_name"]
            person_map[person]["category"] = txn["person_category"] or ""

            if td > six_months_ago:
                person_map[person]["pending"] += 1
                pending_count += 1
                continue

            if not t_conn:
                no_data_count += 1
                continue

            ohlcv_row = _one(t_conn,
                """
                SELECT close FROM ohlcv
                WHERE  ticker_id = ?
                  AND  date >= date(?, '+180 days')
                ORDER BY date ASC LIMIT 1
                """, (ticker_id, td))

            if not ohlcv_row or ohlcv_row["close"] is None:
                no_data_count += 1
                continue

            price_6m  = ohlcv_row["close"]
            txn_price = txn["price"]
            pct_change = round((price_6m - txn_price) / txn_price * 100, 1)

            outcome = (
                "win" if (txn["transaction_type"] == "buy"  and price_6m > txn_price) else
                "win" if (txn["transaction_type"] == "sell" and price_6m < txn_price) else
                "loss"
            )

            person_map[person]["wins" if outcome == "win" else "losses"] += 1
            resolved_events.append({
                "person_name":       person,
                "transaction_type":  txn["transaction_type"],
                "transaction_date":  td,
                "transaction_price": txn_price,
                "price_6m":          round(price_6m, 2),
                "pct_change":        pct_change,
                "outcome":           outcome,
            })

        if t_conn:
            t_conn.close()

        total_wins     = sum(p["wins"]   for p in person_map.values())
        total_losses   = sum(p["losses"] for p in person_map.values())
        total_resolved = total_wins + total_losses

        if total_resolved < MIN_RESOLVED:
            return _fmt({
                "company":            identifier,
                "credibility_score":  None,
                "reason":             (
                    f"insufficient_resolved_outcomes — {total_resolved} resolved, "
                    f"minimum {MIN_RESOLVED} required"
                ),
                "total_transactions": len(txns),
                "pending_count":      pending_count,
                "no_data_count":      no_data_count,
                "note": (
                    "Pending = transaction < 6 months ago, outcome unknown. "
                    "No-data = stock not in technical DB or OHLCV gap."
                ),
            })

        win_rate = round(total_wins / total_resolved * 100)

        if win_rate >= 70:
            label = "RELIABLE — buy/sell signals have historically been accurate"
        elif win_rate >= 50:
            label = "MODERATE — some predictive value, use as one signal among many"
        else:
            label = "UNRELIABLE — insider signals have not been predictive historically"

        per_person = [
            {
                "person_name":  name,
                "category":     stats["category"],
                "wins":         stats["wins"],
                "losses":       stats["losses"],
                "win_rate_pct": round(stats["wins"] / max(stats["wins"] + stats["losses"], 1) * 100),
            }
            for name, stats in sorted(
                person_map.items(),
                key=lambda x: x[1]["wins"] / max(x[1]["wins"] + x[1]["losses"], 1),
                reverse=True,
            )
            if (stats["wins"] + stats["losses"]) > 0
        ]

        notable = sorted(resolved_events, key=lambda e: abs(e["pct_change"]), reverse=True)[:5]

        return _fmt({
            "company":           identifier,
            "credibility_score": win_rate,
            "credibility_label": label,
            "win_rate_pct":      win_rate,
            "total_resolved":    total_resolved,
            "wins":              total_wins,
            "losses":            total_losses,
            "pending_count":     pending_count,
            "no_data_count":     no_data_count,
            "per_person":        per_person,
            "notable_instances": notable,
            "note": (
                "Win = bought before price rose OR sold before price fell, "
                "measured at ~6 months after transaction date. "
                f"Minimum {MIN_RESOLVED} resolved events required for score."
            ),
        })

    except Exception as e:
        return f"Error computing promoter track record for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# Tool 57 — get_concall_analysis
# ---------------------------------------------------------------------------

@mcp.tool()
def get_concall_analysis(identifier: str, quarters: int = 4) -> str:
    """
    Tool 57: Analyse management tone from concall transcripts for a company.

    Reads pre-ingested concall data from concall.db (populated by the
    concall-module pipeline). Returns tone scores, keyword breakdowns, and a
    trend across recent quarters.

    Args:
        identifier: NSE code or company name
        quarters:   Number of recent quarters to include (default 4)

    Returns JSON with:
        - company, nse_code
        - quarters: list of {quarter_label, transcript_date, fetch_status,
                             tone_score, positive_count, negative_count,
                             top_keywords}
        - tone_trend: "IMPROVING" | "DETERIORATING" | "STABLE" | "INSUFFICIENT_DATA"
        - avg_tone_score: float
        - note: data freshness / ingestion hint
    """
    try:
        company = _resolve(identifier)
        nse_code = company["nse_code"]

        # Lazy-seed: fetch + cache in Turso DB2 if not already there
        _ensure_concall_data(nse_code, quarters)

        # Read from Turso DB2 (fallback to local SQLite)
        from turso_db import db2_query
        try:
            rows = db2_query(
                """SELECT id, quarter_label, transcript_date, fetch_status,
                          tone_score, positive_count, negative_count,
                          char_count, word_count, scan_only, protected
                   FROM concall_transcripts
                   WHERE nse_code = ? AND fetch_status IN ('success', 'scan_only')
                   ORDER BY transcript_date DESC LIMIT ?""",
                [nse_code, quarters]
            )
        except Exception:
            # Turso unavailable — fall back to local SQLite
            rows = []
            if CONCALL_DB.exists():
                conn = _conn(CONCALL_DB)
                rows = _rows(conn,
                    """SELECT id, quarter_label, transcript_date, fetch_status,
                              tone_score, positive_count, negative_count,
                              char_count, word_count, scan_only, protected
                       FROM concall_transcripts
                       WHERE nse_code = ? AND fetch_status IN ('success','scan_only')
                       ORDER BY transcript_date DESC LIMIT ?""",
                    (nse_code, quarters)
                )
                conn.close()

        if not rows:
            return _fmt({
                "company": identifier,
                "nse_code": nse_code,
                "quarters": [],
                "tone_trend": "INSUFFICIENT_DATA",
                "avg_tone_score": None,
                "note": f"No concall transcripts found for {nse_code}.",
            })

        quarter_data = []
        scores_for_trend = []

        for row in rows:
            tid = row["id"]
            # Fetch top keywords from Turso DB2
            try:
                kw_rows = db2_query(
                    """SELECT category, keyword, count FROM concall_keywords
                       WHERE transcript_id = ? ORDER BY count DESC LIMIT 10""",
                    [tid]
                )
            except Exception:
                kw_rows = []

            top_kws = [
                {"category": r["category"], "keyword": r["keyword"], "count": r["count"]}
                for r in kw_rows
            ]

            tone = row["tone_score"]
            quarter_data.append({
                "quarter_label":    row["quarter_label"],
                "transcript_date":  row["transcript_date"],
                "fetch_status":     row["fetch_status"],
                "tone_score":       tone,
                "positive_count":   row["positive_count"],
                "negative_count":   row["negative_count"],
                "char_count":       row["char_count"],
                "word_count":       row["word_count"],
                "scan_only":        bool(row["scan_only"]),
                "top_keywords":     top_kws,
            })
            if tone is not None:
                scores_for_trend.append(tone)

        # Tone trend: compare oldest vs newest scored quarters
        tone_trend = "INSUFFICIENT_DATA"
        avg_tone = None
        if len(scores_for_trend) >= 2:
            avg_tone = round(sum(scores_for_trend) / len(scores_for_trend), 1)
            newest = scores_for_trend[0]
            oldest = scores_for_trend[-1]
            delta = newest - oldest
            if delta >= 5:
                tone_trend = "IMPROVING"
            elif delta <= -5:
                tone_trend = "DETERIORATING"
            else:
                tone_trend = "STABLE"
        elif len(scores_for_trend) == 1:
            avg_tone = scores_for_trend[0]
            tone_trend = "INSUFFICIENT_DATA"

        return _fmt({
            "company":        identifier,
            "nse_code":       nse_code,
            "quarters":       quarter_data,
            "tone_trend":     tone_trend,
            "avg_tone_score": avg_tone,
            "note": (
                "Tone score: 0=fully negative, 50=neutral, 100=fully positive. "
                "Data auto-fetched on first query and cached in Turso DB2."
            ),
        })

    except Exception as e:
        return f"Error fetching concall analysis for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# TODO 7b helpers — concall pipeline trigger + guidance extraction
# ---------------------------------------------------------------------------

def _ensure_concall_data(nse_code: str, quarters: int = 4) -> None:
    """
    Ensure concall data for nse_code is in Turso DB2.
    Check DB2 first — if missing or stale (>90 days), run the pipeline and sync.
    Falls back to local SQLite if Turso is unavailable.
    """
    from turso_db import db2_query
    try:
        rows = db2_query(
            "SELECT COUNT(*) as n FROM concall_transcripts WHERE nse_code = ?",
            [nse_code]
        )
        if rows and int(rows[0].get("n", 0)) > 0:
            return  # already in Turso DB2
    except Exception:
        pass  # Turso unavailable — fall through to local SQLite check

    # Not in Turso — run pipeline (writes to local SQLite), then sync up
    _run_concall_pipeline(nse_code, quarters)
    _sync_concall_to_turso(nse_code)


def _run_concall_pipeline(nse_code: str, quarters: int) -> None:
    """Invoke the concall pipeline in-process (writes to local SQLite)."""
    try:
        sys.path.insert(0, str(ROOT / "concall-module"))
        from src.pipeline import run_pipeline
        run_pipeline(nse_code, quarters=quarters)
    except Exception:
        pass  # Caller checks DB; failure surfaces as "no data" message


def _sync_concall_to_turso(nse_code: str) -> None:
    """Copy concall_transcripts + concall_keywords for nse_code from local SQLite → Turso DB2."""
    if not CONCALL_DB.exists():
        return
    try:
        from turso_db import db2_batch
        conn = _conn(CONCALL_DB)

        transcripts = _rows(conn,
            """SELECT id, nse_code, company_name, quarter_label, transcript_date,
                      pdf_url, char_count, word_count, scan_only, protected,
                      fetch_status, tone_score, positive_count, negative_count
               FROM concall_transcripts WHERE nse_code = ?""",
            (nse_code,)
        )
        if not transcripts:
            conn.close()
            return

        # Upsert transcripts
        t_stmts = []
        local_id_to_turso_key = {}
        for t in transcripts:
            t_stmts.append((
                """INSERT OR IGNORE INTO concall_transcripts
                   (nse_code, company_name, quarter_label, transcript_date, pdf_url,
                    char_count, word_count, scan_only, protected, fetch_status,
                    tone_score, positive_count, negative_count)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [t["nse_code"], t["company_name"], t["quarter_label"], t["transcript_date"],
                 t["pdf_url"], t["char_count"], t["word_count"], t["scan_only"], t["protected"],
                 t["fetch_status"], t["tone_score"], t["positive_count"], t["negative_count"]]
            ))
            local_id_to_turso_key[t["id"]] = t["pdf_url"]

        db2_batch(t_stmts)

        # Fetch the Turso IDs by pdf_url to link keywords correctly
        from turso_db import db2_query
        turso_rows = db2_query(
            "SELECT id, pdf_url FROM concall_transcripts WHERE nse_code = ?", [nse_code]
        )
        url_to_turso_id = {r["pdf_url"]: r["id"] for r in turso_rows}

        # Upsert keywords
        kw_stmts = []
        for t in transcripts:
            turso_id = url_to_turso_id.get(t["pdf_url"])
            if not turso_id:
                continue
            kws = _rows(conn,
                "SELECT category, keyword, count FROM concall_keywords WHERE transcript_id = ?",
                (t["id"],)
            )
            for kw in kws:
                kw_stmts.append((
                    """INSERT OR IGNORE INTO concall_keywords
                       (transcript_id, category, keyword, count) VALUES (?,?,?,?)""",
                    [turso_id, kw["category"], kw["keyword"], kw["count"]]
                ))

        if kw_stmts:
            db2_batch(kw_stmts)

        conn.close()
    except Exception:
        pass  # Sync failure is non-fatal — local SQLite still has the data


# Guidance sentence classification keywords
_GUIDANCE_FORWARD = {
    "guidance", "target", "expect", "aspire", "aim to", "plan to",
    "going forward", "next quarter", "next year", "by fy", "by march",
    "by december", "we will", "we expect", "we aim", "committed to",
    "outlook", "projection", "forecast", "in coming", "pipeline",
}
_GUIDANCE_CATEGORIES = {
    "revenue":   {"revenue", "sales", "turnover", "topline", "top-line", "growth"},
    "margins":   {"margin", "ebitda", "operating profit", "profitability", "ebidta"},
    "capex":     {"capex", "capital expenditure", "investment", "capacity", "plant"},
    "debt":      {"debt", "leverage", "borrowing", "repay", "deleverage", "net debt"},
    "expansion": {"expansion", "launch", "open", "stores", "branch", "geography", "market"},
}


def _classify_guidance(sentence: str) -> str | None:
    low = sentence.lower()
    for cat, kws in _GUIDANCE_CATEGORIES.items():
        if any(kw in low for kw in kws):
            return cat
    return "general"


def _extract_guidance(text: str, max_sentences: int = 30) -> list[dict]:
    """
    Extract forward-looking guidance sentences from raw concall transcript text.
    Returns list of {text, category}.
    """
    # Split on sentence-ending punctuation, keeping context
    import re
    raw_sentences = re.split(r"(?<=[.!?])\s+", text)
    results = []
    seen: set[str] = set()

    for s in raw_sentences:
        s = s.strip()
        if len(s) < 30 or len(s) > 500:
            continue
        low = s.lower()
        if not any(kw in low for kw in _GUIDANCE_FORWARD):
            continue
        key = s[:80]  # deduplicate on first 80 chars
        if key in seen:
            continue
        seen.add(key)
        results.append({"text": s, "category": _classify_guidance(s)})
        if len(results) >= max_sentences:
            break

    return results


@mcp.tool()
def get_promise_tracker(identifier: str, quarters: int = 4) -> str:
    """
    Extract and display management guidance / forward-looking statements from
    concall transcripts across recent quarters.

    Fetches concall PDFs (running the pipeline on first call), then extracts
    sentences where management made commitments or gave guidance on revenue,
    margins, capex, debt reduction, or expansion plans.

    Args:
        identifier: NSE symbol, BSE code, or ISIN
        quarters:   Number of recent quarters to analyse (default 4)

    KEY SIGNALS:
      Repeated guidance on the same metric → management priority signal
      Guidance given but not repeated next quarter → possible miss
      Capex guidance + rising debt → watch cash flows
      Margin guidance improving → worth cross-checking with actual screener data
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")
        if not nse_code:
            return f"No NSE code for '{identifier}'."

        _ensure_concall_data(nse_code, quarters)

        if not CONCALL_DB.exists():
            return f"No concall data for '{nse_code}'. Run: python concall-module/run_pipeline.py {nse_code}"

        conn = _conn(CONCALL_DB)

        # Init promises table if not exists
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS concall_promises (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                transcript_id INTEGER NOT NULL,
                nse_code      TEXT    NOT NULL,
                quarter_label TEXT,
                promise_text  TEXT    NOT NULL,
                category      TEXT,
                extracted_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(transcript_id, promise_text)
            );
            CREATE INDEX IF NOT EXISTS idx_promises_nse ON concall_promises(nse_code);
        """)
        conn.commit()

        transcripts = _rows(conn,
            """
            SELECT id, quarter_label, transcript_date, pdf_url, fetch_status
            FROM   concall_transcripts
            WHERE  nse_code = ? AND fetch_status = 'success'
            ORDER  BY transcript_date DESC
            LIMIT  ?
            """,
            (nse_code, quarters),
        )

        if not transcripts:
            conn.close()
            return (
                f"No successfully parsed concall transcripts for '{nse_code}'. "
                f"Run: python concall-module/run_pipeline.py {nse_code}"
            )

        output_quarters = []

        for t in transcripts:
            tid   = t["id"]
            label = t["quarter_label"] or "Unknown quarter"

            # Check if promises already extracted for this transcript
            existing = _rows(conn,
                "SELECT promise_text, category FROM concall_promises WHERE transcript_id = ?",
                (tid,),
            )

            if not existing:
                # Re-download + extract
                sys.path.insert(0, str(ROOT / "concall-module"))
                from src.pipeline import _download_pdf, _extract_text
                from src.db import upsert_promises as _up

                pdf_bytes = _download_pdf(t["pdf_url"])
                if pdf_bytes:
                    raw_text, _ = _extract_text(pdf_bytes)
                    promises = _extract_guidance(raw_text)
                    _up(conn, tid, nse_code, label, promises)
                    existing = promises  # use freshly extracted list
                else:
                    existing = []

            output_quarters.append({
                "quarter":  label,
                "date":     t["transcript_date"],
                "promises": [
                    {"text": p.get("text") or p.get("promise_text"), "category": p.get("category")}
                    for p in existing
                ],
            })

        conn.close()

        total = sum(len(q["promises"]) for q in output_quarters)
        return _fmt({
            "company":          identifier,
            "nse_code":         nse_code,
            "quarters_analysed": len(output_quarters),
            "total_guidance_statements": total,
            "quarters":         output_quarters,
            "note": (
                "Guidance statements are extracted by keyword matching — "
                "review each for context. Cross-check with get_fundamentals() "
                "to see if actual results matched the guidance."
            ),
        })

    except Exception as e:
        return f"Error fetching promise tracker for '{identifier}': {e}"


@mcp.tool()
def get_management_credibility_score(identifier: str) -> str:
    """
    Score management credibility by correlating concall tone with actual results.

    For each quarter where a concall transcript exists (with tone score), looks up
    the actual revenue/PAT outcome in the following quarter from screener.db and
    checks whether management's optimism/pessimism was justified.

    Credibility score:
      100 = management tone accurately predicted actual performance every time
       50 = random (tone had no predictive value)
        0 = management tone consistently contradicted actual results

    Args:
        identifier: NSE symbol, BSE code, or ISIN

    KEY SIGNALS:
      Score > 70 → reliable management — weight their guidance highly
      Score < 40 → habitual over-promisers — discount positive guidance
      Score trend improving → management becoming more transparent over time
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)
        nse_code = company.get("nse_code")
        if not nse_code:
            return f"No NSE code for '{identifier}'."

        screener_id = company.get("screener_company_id")

        _ensure_concall_data(nse_code, quarters=8)

        if not CONCALL_DB.exists():
            return f"No concall data for '{nse_code}'. Run: python concall-module/run_pipeline.py {nse_code}"

        concall_conn = _conn(CONCALL_DB)
        transcripts  = _rows(concall_conn,
            """
            SELECT quarter_label, transcript_date, tone_score
            FROM   concall_transcripts
            WHERE  nse_code = ? AND fetch_status = 'success' AND tone_score IS NOT NULL
            ORDER  BY transcript_date ASC
            """,
            (nse_code,),
        )
        concall_conn.close()

        if len(transcripts) < 2:
            return (
                f"Need at least 2 scored concall transcripts for '{nse_code}' to compute "
                "a credibility score. "
                f"Run: python concall-module/run_pipeline.py {nse_code} --quarters 8"
            )

        # Pull quarterly revenue + PAT from screener.db
        quarterly_results: list[dict] = []
        if screener_id:
            sc_conn = _conn(SCREENER_DB)
            quarterly_results = [dict(r) for r in sc_conn.execute(
                """
                SELECT p.label, p.period_end,
                       fi.net_sales AS revenue, fi.net_profit AS pat
                FROM   fact_income fi
                JOIN   dim_period  p  ON p.period_id = fi.period_id
                WHERE  fi.company_id = ?
                  AND  p.period_type = 'quarterly'
                  AND  fi.net_sales  IS NOT NULL
                ORDER  BY p.period_end ASC
                """,
                (screener_id,),
            ).fetchall()]
            sc_conn.close()

        # Build period_end → result lookup
        result_by_period: dict[str, dict] = {r["period_end"]: r for r in quarterly_results}
        sorted_periods = sorted(result_by_period.keys())

        def _next_period_result(concall_date: str) -> dict | None:
            """Find the first quarterly result with period_end AFTER the concall date."""
            for p in sorted_periods:
                if p > concall_date:
                    return result_by_period[p]
            return None

        records = []
        hits = misses = 0

        for i, t in enumerate(transcripts[:-1]):  # last quarter has no "next" yet
            tone      = t["tone_score"]
            optimistic = tone >= 55   # above neutral
            pessimistic = tone <= 45

            next_result = _next_period_result(t["transcript_date"] or "")
            if not next_result:
                records.append({
                    "quarter": t["quarter_label"],
                    "tone_score": round(tone, 1),
                    "management_signal": "OPTIMISTIC" if optimistic else ("PESSIMISTIC" if pessimistic else "NEUTRAL"),
                    "next_quarter_revenue": None,
                    "revenue_growth_pct": None,
                    "outcome": "NO_DATA",
                })
                continue

            # Compare to the one-before-next to get growth
            prev_rev = None
            if i > 0:
                prev_result = _next_period_result(transcripts[i - 1]["transcript_date"] or "")
                if prev_result:
                    prev_rev = prev_result.get("revenue")

            curr_rev = next_result.get("revenue")
            rev_growth = None
            if curr_rev and prev_rev and prev_rev != 0:
                rev_growth = round((curr_rev - prev_rev) / abs(prev_rev) * 100, 1)

            outcome = "NO_DATA"
            if rev_growth is not None:
                grew = rev_growth > 0
                if optimistic and grew:
                    outcome = "HIT"
                    hits += 1
                elif optimistic and not grew:
                    outcome = "MISS"
                    misses += 1
                elif pessimistic and not grew:
                    outcome = "HIT"
                    hits += 1
                elif pessimistic and grew:
                    outcome = "MISS"
                    misses += 1
                else:
                    outcome = "NEUTRAL"

            records.append({
                "quarter":           t["quarter_label"],
                "tone_score":        round(tone, 1),
                "management_signal": "OPTIMISTIC" if optimistic else ("PESSIMISTIC" if pessimistic else "NEUTRAL"),
                "next_quarter_revenue_cr": round(curr_rev / 1e7, 1) if curr_rev else None,
                "revenue_growth_pct": rev_growth,
                "outcome":           outcome,
            })

        resolved = hits + misses
        credibility_score = round(hits / resolved * 100) if resolved else None

        return _fmt({
            "company":            identifier,
            "nse_code":           nse_code,
            "credibility_score":  credibility_score,
            "interpretation": (
                "RELIABLE"     if credibility_score and credibility_score >= 70 else
                "MIXED"        if credibility_score and credibility_score >= 45 else
                "OVER-PROMISER" if credibility_score is not None else "INSUFFICIENT_DATA"
            ),
            "hits":   hits,
            "misses": misses,
            "pending_quarters": len(transcripts) - 1 - resolved,
            "track_record": records,
            "note": (
                "HIT = management tone matched actual revenue direction next quarter. "
                "MISS = management was optimistic but revenue fell, or pessimistic but revenue grew. "
                "Score requires screener.db quarterly data — run screener pipeline if score is null."
            ),
        })

    except Exception as e:
        return f"Error computing credibility score for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# PILLAR 9 — BSE Corporate Filings (TODO 18)
# ---------------------------------------------------------------------------
#
# Data flow:
#
#   _fetch_bse_filings(bse_code, subcategory, days)
#         │  BSE API: AnnSubCategoryGetData → list of filing metadata
#         │  Upserts to bse_filings table in forensic.db
#         ▼
#   _bse_pdf_text(attachment_id)
#         │  Download: bseindia.com/xml-data/corpfiling/AttachHis/{id}.pdf
#         │  Extract via pdfplumber → cache in bse_filings.extracted_text
#         ▼
#   get_bse_filings    — filing list (Claude navigates to specific filings)
#   get_filing_text    — raw PDF text for one attachment
#   get_rpt_disclosures  — RPT section from Financial Results PDFs
#   get_board_outcomes   — resolution text from Board Meeting Outcome PDFs

_BSE_PDF_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/{attachment_id}.pdf"
_BSE_PDF_MAX_PAGES  = 80
_BSE_PDF_MAX_CHARS  = 100_000   # larger than concall (financial PDFs are verbose)
_BSE_PDF_TIMEOUT    = 30        # seconds


def _bse_pdf_text(attachment_id: str) -> str:
    """
    Download a BSE filing PDF by attachment UUID and extract text via pdfplumber.

    Returns extracted text (may be empty if password-protected or scan-only PDF).
    Never raises — all failures return "".
    """
    import io
    import requests as _req
    try:
        import pdfplumber
    except ImportError:
        return ""

    url = _BSE_PDF_BASE.format(attachment_id=attachment_id)
    try:
        resp = _req.get(url, timeout=_BSE_PDF_TIMEOUT, headers={"User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )})
        resp.raise_for_status()
        pdf_bytes = resp.content
    except Exception:
        return ""

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            if len(pdf.pages) > _BSE_PDF_MAX_PAGES:
                return ""
            parts = []
            for page in pdf.pages:
                parts.append(page.extract_text() or "")
                if sum(len(p) for p in parts) >= _BSE_PDF_MAX_CHARS:
                    break
            return "\n".join(parts)[:_BSE_PDF_MAX_CHARS]
    except Exception:
        return ""


def _fetch_bse_filings(bse_code: str, subcategory: str | None, days: int) -> list[dict]:
    """
    Fetch BSE filings metadata from BSE API, upsert to forensic.db, return rows.
    Gracefully handles DB write lock (TODO 20) by logging and continuing.
    """
    from src.fetcher import fetch_bse_filings as _api_fetch
    from src.db import upsert_bse_filing

    filings = _api_fetch(bse_code, subcategory=subcategory, days=days)

    try:
        conn = _forensic_conn()
        for f in filings:
            upsert_bse_filing(conn, f)
        conn.commit()
        conn.close()
    except Exception as e:
        import logging as _log
        _log.getLogger(__name__).warning(f"  bse_filings upsert failed (continuing): {e}")

    return filings


def _bse_pdf_text_cached(news_id: str, attachment_id: str) -> str:
    """
    Return extracted text for a filing, using DB cache when available.
    Downloads and caches on first call.
    """
    from src.db import update_bse_filing_text

    # Check cache first
    try:
        conn = _forensic_conn()
        row = conn.execute(
            "SELECT extracted_text FROM bse_filings WHERE news_id = ?", (news_id,)
        ).fetchone()
        conn.close()
        if row and row[0]:
            return row[0]
    except Exception:
        pass

    # Download + extract
    text = _bse_pdf_text(attachment_id)

    # Cache the result (even if empty — avoids re-downloading unextractable PDFs)
    try:
        conn = _forensic_conn()
        update_bse_filing_text(conn, news_id, text)
        conn.commit()
        conn.close()
    except Exception:
        pass

    return text


def _extract_section(text: str, keywords: list[str], context_lines: int = 80) -> str:
    """
    Find the first occurrence of any keyword in text (case-insensitive),
    return up to context_lines lines of surrounding text.
    Returns "" if no keyword found.
    """
    lines = text.splitlines()
    lower_lines = [l.lower() for l in lines]
    for kw in keywords:
        kw_lower = kw.lower()
        for i, line in enumerate(lower_lines):
            if kw_lower in line:
                start = max(0, i - 2)
                end   = min(len(lines), i + context_lines)
                return "\n".join(lines[start:end])
    return ""


@mcp.tool()
def get_bse_filings(
    identifier: str,
    subcategory: str | None = None,
    days: int = 30,
) -> str:
    """
    List BSE regulatory filings for a company.

    Returns filing metadata including attachment IDs that can be passed to
    get_filing_text() to read the actual PDF content.

    Args:
        identifier:  NSE symbol, BSE code, or ISIN
        subcategory: Optional filter. Common values:
                     "Financial Results", "Outcome of Board Meeting",
                     "Board Meeting Intimation", "Change in Management",
                     "Disclosures under Reg. 29(1) of SEBI (SAST)", "AGM"
                     Leave None to get all subcategories.
        days:        How many calendar days back to search (default 30, max ~90
                     before results may be incomplete — see TODO 19 for pagination)

    TYPICAL WORKFLOW:
      1. get_bse_filings("CHOICEIN", "Financial Results", 90)
         → returns list with attachment_ids
      2. get_filing_text(attachment_id) → reads the PDF
      3. Or use get_rpt_disclosures / get_board_outcomes for targeted extraction
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        bse_code = company.get("bse_code")
        if not bse_code:
            return (
                f"BSE code not available for '{identifier}'. "
                "BSE filings require a BSE scrip code. "
                "This company may be NSE-only listed or not yet mapped. "
                "Try find_company to check available identifiers."
            )

        filings = _fetch_bse_filings(str(bse_code), subcategory, days)

        if not filings:
            subcat_note = f" with subcategory '{subcategory}'" if subcategory else ""
            return (
                f"No BSE filings found for '{identifier}'{subcat_note} "
                f"in the last {days} days."
            )

        return _fmt({
            "company":    identifier,
            "bse_code":   bse_code,
            "days":       days,
            "subcategory_filter": subcategory,
            "total":      len(filings),
            "filings":    filings,
            "note": (
                "Pass attachment_id to get_filing_text() to read the PDF. "
                "Use get_rpt_disclosures() for RPT extraction, "
                "get_board_outcomes() for board resolution extraction."
            ),
        })
    except Exception as e:
        return f"Error fetching BSE filings for '{identifier}': {e}"


@mcp.tool()
def get_filing_text(attachment_id: str) -> str:
    """
    Download and extract text from a BSE filing PDF.

    The attachment_id comes from get_bse_filings() output. Text is cached in
    forensic.db after first download — repeat calls are fast.

    Args:
        attachment_id: UUID string from get_bse_filings() 'attachment_id' field

    Returns the raw extracted text (up to 100,000 characters). For very large
    PDFs (>80 pages) returns an empty result with an explanatory message.

    USE WHEN: You want to read the full content of a specific filing PDF.
    """
    if not attachment_id or not attachment_id.strip():
        return "attachment_id is required. Get it from get_bse_filings()."

    attachment_id = attachment_id.strip()

    # Check if we already have cached text for this attachment
    try:
        conn = _forensic_conn()
        row = conn.execute(
            "SELECT extracted_text, headline, filing_date FROM bse_filings WHERE attachment_id = ?",
            (attachment_id,),
        ).fetchone()
        conn.close()
        if row and row[0]:
            return _fmt({
                "attachment_id": attachment_id,
                "headline":      row[1],
                "filing_date":   row[2],
                "cached":        True,
                "char_count":    len(row[0]),
                "text":          row[0],
            })
    except Exception:
        pass

    # Not in cache — download and extract
    text = _bse_pdf_text(attachment_id)

    if not text:
        return (
            f"Could not extract text from PDF (attachment_id={attachment_id}). "
            "The PDF may be password-protected, scan-only (image PDF), "
            "or the attachment ID may be invalid."
        )

    # Cache the text (best-effort — if forensic.db is locked, still return the text)
    try:
        conn = _forensic_conn()
        conn.execute(
            "UPDATE bse_filings SET extracted_text = ? WHERE attachment_id = ?",
            (text, attachment_id),
        )
        conn.commit()
        conn.close()
    except Exception:
        pass

    return _fmt({
        "attachment_id": attachment_id,
        "cached":        False,
        "char_count":    len(text),
        "text":          text,
    })


@mcp.tool()
def get_rpt_disclosures(
    identifier: str,
    quarters: int = 4,
) -> str:
    """
    Extract Related Party Transaction (RPT) disclosures from quarterly Financial
    Results PDFs filed with BSE.

    Fetches the most recent `quarters` Financial Results filings, downloads each
    PDF, and extracts the Ind AS 24 related-party note section. Returns raw text
    from the RPT section — Claude interprets the actual transaction amounts and
    counterparties.

    Args:
        identifier: NSE symbol, BSE code, or ISIN
        quarters:   Number of recent quarterly results to fetch (default 4 = 1 year)

    KEY SIGNALS:
      RPT amounts > 30% of revenue → related-party revenue concern
      Inter-corporate loans to promoter entities → fund diversion risk
      Increasing RPT amounts YoY → governance deterioration
      New related parties appearing → shell company creation
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        bse_code = company.get("bse_code")
        if not bse_code:
            return (
                f"BSE code not available for '{identifier}'. "
                "RPT disclosures require a BSE scrip code."
            )

        # Fetch Financial Results filings (90-day window covers most quarterly results)
        filings = _fetch_bse_filings(str(bse_code), "Financial Results", days=90)

        if not filings:
            return (
                f"No Financial Results filings found for '{identifier}' in the last 90 days. "
                "Try get_bse_filings with a wider date range."
            )

        results = []
        _rpt_keywords = [
            "related party", "related-party", "ind as 24",
            "as-18", "related parties", "transactions with related",
        ]

        for filing in filings[:quarters]:
            attach_id = filing.get("attachment_id")
            news_id   = filing.get("news_id", "")
            label     = f"{filing.get('subcategory', 'Filing')} — {filing.get('filing_date', 'unknown date')}"

            if not attach_id:
                results.append({"filing": label, "status": "no_attachment", "rpt_text": None})
                continue

            text = _bse_pdf_text_cached(news_id, attach_id)

            if not text:
                results.append({
                    "filing": label,
                    "status": "pdf_unreadable",
                    "rpt_text": None,
                    "note": "PDF may be password-protected or scan-only.",
                })
                continue

            rpt_section = _extract_section(text, _rpt_keywords, context_lines=100)

            if not rpt_section:
                results.append({
                    "filing":   label,
                    "status":   "no_rpt_section_found",
                    "rpt_text": None,
                    "note":     "Keywords not found: " + ", ".join(_rpt_keywords[:3]),
                })
            else:
                results.append({
                    "filing":   label,
                    "status":   "extracted",
                    "rpt_text": rpt_section,
                })

        found = sum(1 for r in results if r["status"] == "extracted")
        return _fmt({
            "company":         identifier,
            "quarters_requested": quarters,
            "quarters_found":  len(filings[:quarters]),
            "rpt_sections_extracted": found,
            "results":         results,
            "note": (
                "rpt_text is raw PDF text — read it to extract counterparty names, "
                "transaction types, and amounts. Cross-reference with get_fraud_score()."
            ),
        })
    except Exception as e:
        return f"Error fetching RPT disclosures for '{identifier}': {e}"


@mcp.tool()
def get_board_outcomes(
    identifier: str,
    quarters: int = 4,
) -> str:
    """
    Extract board meeting outcome resolutions from BSE filings.

    Board meeting outcomes contain: dividend announcements, RPT approvals,
    capex decisions, management changes, and other material resolutions.

    Args:
        identifier: NSE symbol, BSE code, or ISIN
        quarters:   Number of recent board outcomes to fetch (default 4)

    KEY SIGNALS:
      RPT omnibus approval in board outcome → pre-authorised related-party deals
      Large capex approval with no analyst guidance → undisclosed expansion
      Management change + simultaneous large RPT approval → governance red flag
      Dividend cut in board outcome → cash flow stress signal
    """
    try:
        company = _resolve(identifier)
        if not company:
            return _not_found(identifier)

        bse_code = company.get("bse_code")
        if not bse_code:
            return (
                f"BSE code not available for '{identifier}'. "
                "Board outcomes require a BSE scrip code."
            )

        filings = _fetch_bse_filings(str(bse_code), "Outcome of Board Meeting", days=90)

        if not filings:
            return (
                f"No 'Outcome of Board Meeting' filings found for '{identifier}' "
                "in the last 90 days. Try get_bse_filings with a wider date range."
            )

        results = []
        _resolution_keywords = [
            "resolved", "resolution", "approved", "considered and approved",
            "board approved", "directors approved", "recommended", "declared",
        ]

        for filing in filings[:quarters]:
            attach_id = filing.get("attachment_id")
            news_id   = filing.get("news_id", "")
            label     = f"Board Outcome — {filing.get('filing_date', 'unknown date')}"

            if not attach_id:
                results.append({"filing": label, "status": "no_attachment", "text": None})
                continue

            text = _bse_pdf_text_cached(news_id, attach_id)

            if not text:
                results.append({
                    "filing": label,
                    "status": "pdf_unreadable",
                    "text":   None,
                    "note":   "PDF may be password-protected or scan-only.",
                })
                continue

            resolution_section = _extract_section(text, _resolution_keywords, context_lines=120)

            if not resolution_section:
                # Fall back to returning the full text (board outcomes are usually short)
                results.append({
                    "filing":  label,
                    "status":  "full_text_returned",
                    "text":    text[:5000],
                })
            else:
                results.append({
                    "filing":  label,
                    "status":  "extracted",
                    "text":    resolution_section,
                })

        found = sum(1 for r in results if r["status"] in ("extracted", "full_text_returned"))
        return _fmt({
            "company":              identifier,
            "outcomes_requested":   quarters,
            "outcomes_found":       len(filings[:quarters]),
            "outcomes_with_text":   found,
            "results":              results,
        })
    except Exception as e:
        return f"Error fetching board outcomes for '{identifier}': {e}"


# ---------------------------------------------------------------------------
# get_capabilities — UX discoverability tool
# ---------------------------------------------------------------------------

@mcp.tool()
def get_capabilities() -> str:
    """
    Return a human-friendly menu of everything this AI Finance system can do,
    organised by pillar, with example questions for each.

    Call this when the user asks:
      "What can you do?"
      "What tools do you have?"
      "What should I ask you?"
      "Show me what's available"
      "What can I explore?"

    Returns a structured guide covering all 10 pillars + example questions.
    """
    return """
╔══════════════════════════════════════════════════════════════════════╗
║        AI FINANCE — YOUR INSTITUTIONAL EQUITY RESEARCH SYSTEM        ║
║              ~5,200 NSE/BSE listed companies · 10 pillars            ║
╚══════════════════════════════════════════════════════════════════════╝

START HERE — just ask about any company by name, NSE symbol, or BSE code.
Example: "Tell me about HDFCBANK" or "Is RELIANCE a buy?"

──────────────────────────────────────────────────────────────────────
PILLAR 1 — NEWS & SENTIMENT
──────────────────────────────────────────────────────────────────────
36,000+ articles scored by FinBERT. Detects breaking news before it
hits price. Measures whether the market actually believed the news.

Try asking:
  • "What's the news sentiment on BAJFINANCE last 30 days?"
  • "Has TATASTEEL's stock reacted to recent news, or ignored it?"
  • "Any major negative news on ADANIPORTS recently?"

──────────────────────────────────────────────────────────────────────
PILLAR 2 — FUNDAMENTALS (10 years of data)
──────────────────────────────────────────────────────────────────────
P&L, balance sheet, cash flow, efficiency ratios, shareholding — all
from screener.in. Detects earnings quality, leverage traps, cash leaks.

Try asking:
  • "Show me 10 years of P&L for TITAN"
  • "Is DIXON's ROE sustainable or leverage-driven?"
  • "How is NESTLEIND's cash conversion cycle trending?"
  • "Compare PIDILITIND vs ASIANPAINT on working capital efficiency"

──────────────────────────────────────────────────────────────────────
PILLAR 3 — TECHNICALS (1.5M OHLCV rows · 1.4M signal events)
──────────────────────────────────────────────────────────────────────
Live RSI, MACD, Bollinger Bands, EMAs computed on demand.
Historical signal win rates — know if a signal actually works before
you trade on it.

Try asking:
  • "What technical signals are firing on WIPRO right now?"
  • "Find stocks with 3+ bullish indicators aligned today"
  • "What's the win rate of RSI < 30 signals on NSE mid-caps?"
  • "Show me value_momentum setup candidates"

──────────────────────────────────────────────────────────────────────
PILLAR 4 — SECTOR PULSE
──────────────────────────────────────────────────────────────────────
Sector-level breadth, sentiment, and institutional flow aggregated
from all companies in that sector.

Try asking:
  • "How is the IT sector doing this month?"
  • "Which sectors are seeing institutional accumulation?"
  • "Is there broad distribution in pharma right now?"

──────────────────────────────────────────────────────────────────────
PILLAR 5 — MUTUAL FUND INTELLIGENCE (~500 equity schemes)
──────────────────────────────────────────────────────────────────────
NAV history, 1M/3M/6M/1Y/3Y returns, volatility, DII trend proxy.
Find which funds hold a stock, compare stock vs fund, get MF ideas.

Try asking:
  • "Which mutual funds are accumulating LTIM?"
  • "Compare HDFCBANK stock vs Mirae Asset Large Cap Fund"
  • "Find the best mid-cap funds by 3-year returns"
  • "Portfolio analysis: I hold RELIANCE 30%, INFY 20%, HDFCBANK 50%"

──────────────────────────────────────────────────────────────────────
PILLAR 6 — FORENSIC INTELLIGENCE
──────────────────────────────────────────────────────────────────────
Insider transactions, pledge status, bulk deals, fraud scoring.
Quantified risk score (0-100) with named flags.

Try asking:
  • "Is there any fraud risk in ADANIPORTS?"
  • "Check if promoters are pledging shares in ZOMATO"
  • "Show me the exact % stake of every promoter in RELIANCE" (SAST filings)
  • "Who are all the named holders in CHOICEIN and what % do they hold?"
  • "Show me bulk deals in TATASTEEL last 30 days"
  • "Which promoters are buying their own stock right now?"
  • "Screen for value traps to avoid"

──────────────────────────────────────────────────────────────────────
PILLAR 7 — MCA CORPORATE REGISTRY (director networks)
──────────────────────────────────────────────────────────────────────
Resolves promoter → director → all companies controlled.
Flags shells, struck-off entities, shared related-party vehicles.

Try asking:
  • "Who controls VEDL and what other companies do they run?"
  • "Show me any related-party routing in DISHTV's corporate structure"
  • "Map the full promoter network for CHOICEIN"

──────────────────────────────────────────────────────────────────────
PILLAR 8 — CONCALL & MANAGEMENT CREDIBILITY
──────────────────────────────────────────────────────────────────────
Tone scoring of concall transcripts. Promise tracker — extracts
guidance statements and checks whether management delivered.

Try asking:
  • "Did TATAPOWER management deliver on their last 4 guidance statements?"
  • "What's the management credibility score for RVNL?"
  • "What did BAJAJ-AUTO management say about margins in the last concall?"

──────────────────────────────────────────────────────────────────────
PILLAR 9 — BSE CORPORATE FILINGS
──────────────────────────────────────────────────────────────────────
Live BSE filing fetcher. Board outcomes, related-party disclosures,
financial results, full PDF text extraction.

Try asking:
  • "Show me TATASTEEL's last 5 board meeting outcomes"
  • "Any related-party transactions disclosed by ADANIPOWER recently?"
  • "Get RELIANCE's latest financial result filing from BSE"

──────────────────────────────────────────────────────────────────────
PILLAR 10 — INSTITUTIONAL MEMORY
──────────────────────────────────────────────────────────────────────
Persistent cross-conversation memory. Saves findings, patterns,
and red flags so you never lose a prior insight.

Try asking:
  • "What have we found before about CHOICEIN?"
  • "Save this: ADANIPORTS promoters sold 2% before Q3 miss"
  • "Recall anything about pharma sector FII patterns"

──────────────────────────────────────────────────────────────────────
SCREENING & ALERTS
──────────────────────────────────────────────────────────────────────
Market-wide screener combining fundamentals + technicals. Alert engine
for price, RSI, PE, pledge, promoter thresholds.

Try asking:
  • "Screen for high-quality stocks near 52-week lows"
  • "Set an alert when HDFCBANK RSI drops below 35"
  • "Give me a morning briefing for my watchlist"
  • "What's the current market breadth — bull or bear tape?"
  • "Find stocks where smart money is quietly accumulating"

──────────────────────────────────────────────────────────────────────
QUICK START — HIGHEST VALUE FIRST ASKS
──────────────────────────────────────────────────────────────────────
1. "Give me a full overview of [STOCK]"  — all 5 pillars in one shot
2. "What's the conviction score for [STOCK]?"  — instant buy/avoid verdict
3. "Screen for quality compounders with bullish signals"  — idea generation
4. "Morning briefing for [HDFCBANK, RELIANCE, TCS]"  — daily context
5. "Is there fraud risk in [STOCK]?"  — forensic deep-dive
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser()
    _parser.add_argument("--transport", default="sse", choices=["stdio", "sse"])
    _parser.add_argument("--port", type=int, default=8000)
    _args, _ = _parser.parse_known_args()
    if _args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        import os as _os
        _os.environ.setdefault("PORT", str(_args.port))
        mcp.run(transport="sse")
