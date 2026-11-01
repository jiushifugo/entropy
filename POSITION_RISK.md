# Position risk / 逐仓风险保护

This is telemetry, an experimental opt-in entry gate and opt-in early exit,
**not complete liquidation prevention**. Automatic early exits default OFF.
Do not restart live trading on the assumption that this fixes liquidation risk.

默认仅监测，不自动平仓、加保证金、改杠杆，也不解除人工核验锁。
服务器配置不自动更新。不要只为启用监测而清除强平锁恢复实盘。

## Standalone read-only verification / 独立只读验证

Keep the live engine stopped and the position-review lock intact. Use the
public trading-account address, NOT the agent signer or private key:

```bash
python3 tools/monitor_position_risk.py --account 0xYOUR_PUBLIC_ACCOUNT --once
# Continuous stdout only, Ctrl+C to stop (15s interval):
python3 tools/monitor_position_risk.py --account 0xYOUR_PUBLIC_ACCOUNT
```

This independent entry point uses only allowlisted public `/info` request types. It
does not instantiate Engine, load `.env`, initialize signers, subscribe to
orders, write logs/config/state/locks, or send exchange requests. An error
prints UNKNOWN; `--once` returns nonzero. FLAT alone does not validate hypothetical
margin. Do not open a position just to obtain telemetry. This checks Entropy
risk only, not Arcus positions or pending orders.

An optional hypothetical preflight also reads `activeAssetData` for selected
isolated leverage, directional size limits and available margin. Example
numbers below are inputs for a READ-ONLY calculation, not an order request:

```bash
python3 tools/monitor_position_risk.py --account 0xYOUR_PUBLIC_ACCOUNT --once \
  --preflight --quantity 0.04 --fee-bps 0.9 --slippage-bps 4 --min-distance-pct 3
```

It evaluates hypothetical buy/sell limits around the queried MARK, not the
actual executable book. A `reason: null` is only a model pass, not permission
to resume live trading. A non-null reason means blocked/unmodeled; missing
entry evidence yields nonzero exit in once mode. No hypothetical orders are sent.

Live mode polls Entropy account state every 5 seconds (plus request time).
Record-only mode does not query account risk. The adapter uses its configured
query account and HIP-3 dex, `clearinghouseState` position fields, and
`metaAndAssetCtxs` mark price. No book mid or oracle substitute is used.
Requests can fail; this is not a real-time guarantee or atomic snapshot across
both endpoints. Account source timestamp must be present and at most 15s old.

Displayed values: margin mode, configured leverage, `marginUsed` (API margin
usage, **not** free collateral), mark price, reported liquidation price, and
signed distance to liquidation:

- Long: `(mark - liquidation) / mark * 100`.
- Short: `(liquidation - mark) / mark * 100`.
- A negative value is already beyond the reported liquidation level.
- Missing/invalid data is UNKNOWN, old data STALE, explicit empty positions
  FLAT. FLAT is not proof that a future order has adequate isolated margin.

Configuration under `risk` (defaults, no threshold recommendation):

```yaml
position_risk_guard_enabled: false
position_risk_min_distance_pct: 0.0
position_risk_max_age_sec: 15.0
position_risk_exit_enabled: false
position_risk_exit_distance_pct: 0.0
position_risk_paired_dust_max_usd: 0.0
```

With the guard OFF the strategy's admission logic is unchanged; only read-only
telemetry requests and UI/log output are added. With it ON, the distance must
be explicitly positive. Missing/stale/position-mismatched risk data blocks
strategy orders. Valid position-reducing orders are permitted without flipping
through zero; additions at/below the chosen distance are blocked. Cross-margin
entries and position flips remain unmodeled/blocked. Flat entries and additions
also require the single-tier projection described below. A successful fill invalidates the old position
snapshot until refreshed. Emergency hedges and existing halt exits are not
disabled by this gate; manual review locks still take precedence.

This gate does not force liquidation-risk reduction, reserve margin for an
addition, or guarantee a safe resulting liquidation price. Price gaps, funding,
API outages, and execution failures can still result in liquidation.

## Opt-in early exit / 可选提前退出

Activation requires the entry guard enabled and operator-chosen thresholds
`0 < position_risk_exit_distance_pct <= position_risk_min_distance_pct`.
Zero values are deliberately not usable activation defaults. No suggested
thresholds are implied by tests. Existing liquidation locks must not be cleared
merely to try this feature. Keep the server stopped during verification.

Operator-selected configuration in the example file is 3% for BOTH distances,
with BOTH switches still OFF. At distance <=3%, additions are blocked and the
exit latch triggers together; there is no separate warning/entry-block buffer
before exit. These are distances from the current mark to the liquidation price,
NOT 3% account loss, 3% loss from entry, or a guarantee that liquidation can be
avoided. Omitted YAML values still default to zero (no threshold configured).

Fresh isolated-position telemetry at/below the exit distance latches strategy
halt only when it matches settled local inventory and no order/venue lock is
pending. A persistent `.position-review.risk-exit` marker is written before any
exit sends. Persistence failure escalates to manual review. Restart with this
marker also requires manual review; neither a new day nor flat positions resumes
strategy. It is separate from the normal position-review marker: archive BOTH
only after manual verification, never blindly delete safety markers.

The exit loop uses two agreeing reads of BOTH accounts, fresh books, both
routes' readiness, and each venue's actual minimums, then sends bounded IOC
reduce-only orders. It requires opposite, equal-sized inventory; unpaired or
changed inventory escalates to manual review rather than automatically adopting
account values. Entropy closes first, and the hedge closes only the confirmed
Entropy fill (not the original requested quantity). Known zero first-leg fill
does not send the hedge. An unknown outcome, insufficient partial hedge size,
incomplete hedge, or dust escalates to manual review and stops further automatic
sends. An unavailable venue/account read sends nothing that round and logs an
urgent warning; repeated rounds do not guarantee timely execution.

The risk exit is independent of `flatten_on_execution_halt`, remains latched
after flattening, and never overrides liquidation/unknown-order review locks.
Orders are sequential, not atomic. The strategy stays halted even if price
recovers. Once latched, fresh account/route verification permits continued exit
without requiring the dangerous-distance reading to persist. Telemetry poll
interval plus request latency, reconciliation grace, the two-read delay and
10s exit-loop cadence all add delay. Price gaps, rate limits, trading bounds,
API faults and sequential fills can still leave exposure or cause liquidation.

## Entry projection / 开仓与加仓预估

No leverage or collateral mutation is performed. Selected account leverage,
market identity, USDC collateral, mark price, directional maximum size and
available margin must validate. Only single-tier markets are supported. A
missing table is not guessed, EXCEPT the official built-in IDs less than 50
(one tier, max leverage equals ID). Explicit multi-tier tables are blocked.

Before a first order, pending GTC continuation and an IOC retry, the enabled
guard checks projected distance strictly greater than the entry threshold.
Strategy scans use the maximum configured Entropy crossing allowance, not a
favorable top price. Pure reductions are allowed from valid matching position
data; crossing zero is blocked. Disabled guard leaves strategy admission
unchanged and does not request active-asset entry evidence.

For flat inventory, collateral credit is limited to the smaller of mark/limit
notional divided by selected leverage. Nonzero unexplained flat rawUsd is
blocked. For additions, equity is reconstructed from the reported liquidation
price and single-tier maintenance rate; NO new collateral credit is assumed.
Adverse entry-vs-mark PnL, entry fees and an exit-fee reserve are subtracted.
Favorable PnL is not credited. Projected liquidation is solved from equity at
mark and maintenance rate, then compared with the configured distance. Capacity
checks also require enough directional available margin and maximum trade size.

This is intentionally conservative and can block trades that the venue would
accept (especially additions). Mark/account reads are not atomic; the model
does not guarantee allocation, future funding, liquidation behavior, execution
price after API delay or successful exits. A model pass is NOT a live safety
certificate. Snapshot freshness checks and post-fill monitoring still apply.
Unknown fields, stale data, leverage changes or unsupported tiers fail closed.
Server preflight/model verification and live/nonflat telemetry remain unverified;
offline tests do not prove live exit reliability. Keep the liquidation lock
and server stopped. Do not open test positions.

API references:
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margin-tiers
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals

## Optional paired dust observation / 可选已配对尾仓观察

`risk.position_risk_paired_dust_max_usd` is a per-leg USD ceiling; default
0 disables the exception. This cap is independent of the 3% distance settings.
The operator has selected $0.50 per leg; the example file reflects this choice.
The server config is NOT changed automatically. Omitting the key still defaults
to zero/off. This is NOT forgiveness of
all small positions, a flat-position substitution, or a liquidation guarantee.

Only pre-existing equal/opposite residuals, below BOTH venues' executable
minimums and below the configured per-leg ceiling at fresh ask prices, can be
observed as PAIRED-DUST. Two reads of BOTH actual accounts must agree exactly
with settled local inventory (1e-10 comparison tolerance), with no tracked
execution/pending cancel, existing review lock, or active exit. Verification
holds both venue locks. Failed/stale reads remove the observation; account
mismatch locks for manual review. Unknown orders and liquidation incidents
retain their usual manual locks. A stale observation never suppresses an exit.

For this verified bounded state only, crossing the exit-distance threshold does
not create a new permanent risk-exit lock. It sends no orders and neither adds
collateral nor enlarges the position. Existing entry/projection checks remain
unchanged: endangered dust can STILL block additions and flips. This change
does not promise that strategy trading resumes. It does not suppress the
liquidation event lock if the residual is later liquidated.

Old `.position-review` and `.risk-exit` markers are NEVER automatically cleared.
An exit already latched for a larger position stays latched even if only dust
remains; its manual-review rules are unchanged. Operator account/open-order
verification is still required before any manual archival/restart. Do not add
positions just to meet minimum order size or disable the main risk guards.

Startup/revalidation ordering: bounded equal/opposite residual candidates enter
DUST-VERIFY before any asynchronous account verification. The strategy is paused
(including scans, repairs and retries) while waiting for BOTH fresh books and
two account reads. When a book is not ready, the same-symbol Entropy mark is used
ONLY to identify a possible tiny candidate for bounded waiting, never as proof
of the hedge's price or as permission to trade. No exemption exists until both
fresh executable books verify the actual USD caps and minimums. Large, unequal
or one-sided inventory never gets this waiting exception. Waiting longer than
`position_risk_max_age_sec` escalates to manual review (request latency can add
delay); it does not start a blind exit. Failed account reads cannot resume
strategy, and pending verification cannot persist indefinitely or clear old
incident markers. Successful verification transitions to PAIRED-DUST.
