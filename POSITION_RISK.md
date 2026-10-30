# Position-risk phase 1 / 逐仓风险第一阶段

This is telemetry and an experimental opt-in entry gate, **not complete
liquidation prevention**. Automatic early exits are NOT implemented or enabled.
Do not restart live trading on the assumption that this fixes liquidation risk.

第一阶段不自动平仓、加保证金、改杠杆，也不解除人工核验锁。
服务器配置不自动更新。不要只为启用监测而清除强平锁恢复实盘。

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

Before a later automatic-exit phase: verify real snapshots without opening test
positions, choose warning/block/exit thresholds with the operator, validate
both venues' reduce-only paths, and test partial/unknown outcomes, dust,
stale data, concurrent executions and manual-review escalation. Keep the
current liquidation lock and server stopped while this remains unverified.

API references:
- https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
