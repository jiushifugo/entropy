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

This independent entry point uses only two public `/info` request types. It
does not instantiate Engine, load `.env`, initialize signers, subscribe to
orders, write logs/config/state/locks, or send exchange requests. An error
prints UNKNOWN; `--once` returns nonzero. FLAT does not validate hypothetical
margin. Do not open a position just to obtain telemetry. This checks Entropy
risk only, not Arcus positions or pending orders.

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
```

With the guard OFF the strategy's admission logic is unchanged; only read-only
telemetry requests and UI/log output are added. With it ON, the distance must
be explicitly positive. Missing/stale/position-mismatched risk data blocks
strategy orders. Valid position-reducing orders are permitted without flipping
through zero; additions at/below the chosen distance are blocked. Cross-margin
additions and opening from flat are blocked because there is no cross/hypothetical
margin model in this phase. A successful fill invalidates the old position
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

Operator-selected configuration in the example file is 5% for BOTH distances,
with BOTH switches still OFF. At distance <=5%, additions are blocked and the
exit latch triggers together; there is no separate warning/entry-block buffer
before exit. These are distances from the current mark to the liquidation price,
NOT 5% account loss, 5% loss from entry, or a guarantee that liquidation can be
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

This does not calculate post-entry liquidation prices or allocate isolated
collateral. Opening from flat remains blocked with the entry guard enabled.
Live/nonflat telemetry and operator thresholds remain unverified on the server;
offline tests do not prove live exit reliability. Do not open test positions.

API references:
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
