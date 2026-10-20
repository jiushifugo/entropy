# entropy-arb

**[English documentation / 英文文档 → README.md](README.md)**

开源双交易所永续合约套利机器人。其中一条腿永远是 **Entropy**（Hyperliquid 上的
`io` builder dex）；另一条腿作为对冲腿：

| `--hedge` | 交易所 | 计价货币 | 吃单费 | 协议 |
|---|---|---|---|---|
| `lighter` | Lighter 主网 | USDC | 0 bps | zkLighter ws（增量订单簿，异步结算） |
| `lighter-rh` | Lighter Robinhood 链 | **USDG** | 0 bps | zkLighter ws |
| `tradexyz` | Hyperliquid trade.xyz dex | USDC | ~1 bps | HL l2Book，IOC 同步结算 |
| `arcus` | Arcus 永续合约 | USD | 按账户等级 | Arcus ws，IOC 异步结算 |

> **推荐链接** —— 通过以下链接注册即可支持本项目：
> - Entropy — Tier 4 推荐，100% 返佣：<https://entropy.io/?r=yourquantguy>
> - Lighter Robinhood 链：<https://robinhoodchain.lighter.xyz/?referral=QUANT>
> - trade.xyz（Hyperliquid）：<https://app.hyperliquid.xyz/join/QUANTGUY>

当同一品种在一边贵、另一边便宜时，机器人同时在贵的一边卖出、便宜的一边买入
（均为吃单），持有 delta 中性仓位，等溢价回归后反向平仓。所有交易决策使用的
价格都来自**将要实际成交的那个交易所的真实订单簿**——Hyperliquid 的盘口来自
官方 websocket（`wss://api.hyperliquid.xyz/ws`），Lighter 的盘口来自 Lighter
官方 websocket。

机器人运行期间（即使没有密钥、没有开策略）会自动把两边盘口记录成**分钟级
CSV 数据**，配套的分析工具可以直接把这些数据变成策略所需的三个核心参数。

## 信号逻辑

整个信号就是 `config.yaml` 里三个数字，由你根据采集的数据自己设定：

```
premium_bps =（Entropy 价格 / 对冲腿价格 − 1）× 10 000

                          ┌──────────────  卖出 Entropy + 买入对冲腿
midline + upper  ───────────────────────────────────────────────────
                                       ▲
midline          ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┼ ─ ─   溢价的长期中枢
                                       ▼
midline − lower  ───────────────────────────────────────────────────
                          └──────────────  买入 Entropy + 卖出对冲腿
```

- `midline_bps` —— 溢价的常态水平。跨所溢价几乎从不以零为中心（预言机不同、
  计价货币不同、新上市溢价等），零中心的带只会朝一个方向开仓、打满仓位上限、
  永远无法平仓。请实际测量溢价所在的位置，然后填入。
- `upper_bps` / `lower_bps` —— 中枢上下两侧的入场带宽。

两个方向的门槛都作用于**可实际成交的价格**（Entropy 买一 对 对冲腿卖一，
反之亦然），并且是**扣除双边吃单手续费之后的净门槛**——引擎会在阈值之上
另行叠加手续费。两腿串行延迟保护仅在你显式开启时才会叠加，默认值为 0，
因为它应基于实际成交数据校准。

有一点必须理解：当 `midline_bps: 5` 时，买入 Entropy 的门槛是
`lower − midline`，可能为**负数**。这是有意为之——如果 Entropy 长期贵 5 bps，
那么在溢价为 0 时买入它，相对其自身均衡水平就是便宜了 5 bps，这笔交易正是
此前在 `midline + upper` 处卖出的获利平仓。这同时意味着**中枢填错就是亏钱
策略**：若真实溢价中枢是 0 而你填了 5，机器人会整天以公允价买入 Entropy。
先测量、再交易——数据采集器和分析工具就是为此而生。

## 快速开始

```bash
git clone https://github.com/jiushifugo/entropy.git && cd entropy
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # 数据采集只需要这些

cp config.example.yaml config.yaml       # 策略配置（阈值、规模、风控）
cp .env.example .env                     # 密钥——交易必填
```

交易哪个市场**不在**配置文件中——每次启动时用命令行参数显式指定：
`--symbol`（两个交易所共同交易的品种）和 `--hedge`：
`lighter`、`lighter-rh`、`tradexyz`、`arcus`；Entropy 永远是
另一条腿）。

本机器人**没有模拟盘**——要么采集数据（`--record-only`），要么实盘交易。
请用采集的数据和最小的仓位上限来验证策略，而不是模拟成交。

**第一步：先采集数据**（不需要任何密钥）：

```bash
python3 main.py --record-only --symbol SNDK --hedge lighter-rh
```

至少运行几个小时（最好一整天——溢价存在日内规律），数据写入
`logs/minutes.csv`。

**第二步：分析数据、设定阈值：**

```bash
python3 tools/analyze.py
```

它会输出溢价分布、各档带宽的历史触发频率，以及可直接粘贴进
`config.yaml` 的 `thresholds:` 配置块。

**第三步：实盘** —— 填写 `.env`，安装签名 SDK，仓位上限从刚好满足
交易所最小名义的水平开始：

```bash
pip install -r requirements-live.txt
python3 main.py --symbol SNDK --hedge lighter-rh
```

不带 `--record-only` 运行时，只要两边行情就绪且溢价越过带宽，就会立即
发送真实订单。

**仪表盘。** 在终端运行时会显示实时 Rich 仪表盘：两边盘口（含数据龄/点差）、
持仓与上限、账户权益与本次会话盈亏、两个方向的可成交溢价对比完整门槛
（已含手续费与库存加价，● 表示已武装）、数据采集进度、最近成交，以及日志
尾部（完整日志写入 `logging.file`，默认 `logs/engine.log`）。`--record-only`
模式同样可用。加 `--cn` 参数可使仪表盘全部以中文显示。`--no-dashboard`
可切换为纯日志输出（nohup/systemd 等非终端环境会自动退回纯日志），也可
设置 `logging.dashboard: false`。

## 数据采集与分析

采集器在所有模式下自动运行（`recorder.enabled: true`）：每秒采样一次两边
的真实盘口，每分钟写一行：

| 列 | 含义 |
|---|---|
| `minute_ts`, `time_utc` | 分钟起点（epoch 秒 / ISO UTC） |
| `entropy_bid/ask`, `hedge_bid/ask` | 该分钟最后一次有效盘口 |
| `premium_open/high/low/close/mean/std_bps` | Entropy 相对对冲腿的中间价溢价 |
| `sell_edge_mean/max_bps` | 卖出 Entropy 方向的可成交溢价（Entropy 买一 / 对冲腿卖一 − 1） |
| `buy_edge_mean/max_bps` | 买入 Entropy 方向的可成交溢价（对冲腿买一 / Entropy 卖一 − 1） |
| `samples` | 该分钟约 60 秒中两边盘口同时有效的秒数 |

采集的 edge 为费前口径；分析工具在统计触发频率前会先扣除 `--fees-bps`
（请传入**两边吃单费之和**——零费交易所默认 0.0，对冲腿为 `tradexyz` 时
约为 1.0），因此其表格与建议值可直接填入配置。`--hours 24`
可只分析最近数据；溢价中枢会漂移，请定期重新分析并更新 `config.yaml`。

## 配置说明

策略在 `config.yaml`（严格校验——未知键名直接报错），密钥在 `.env`。
交易市场由命令行指定（`--symbol`、`--hedge`）。完整的双语注释参考：
[config.example.yaml](config.example.yaml)。核心项：

| 键 | 含义 | 默认值 |
|---|---|---|
| `thresholds.midline_bps` | 溢价中枢（必须实测！） | — |
| `thresholds.upper_bps` / `lower_bps` | 入场带宽（> 0） | — |
| `entropy.dex` | Entropy 在 Hyperliquid 上的 dex 名 | `io` |
| `*.taker_fee_bps` | 各所吃单费 | 0.0（tradexyz 对冲腿：1.0） |
| `*.max_position_usd` | 各所持仓上限 | 1000 |
| `*.max_orders_per_min` | 各所每分钟下单预算（滑动 60 秒） | 120；Lighter 对冲腿 30 |
| `sizing.take_fraction` | 吃掉可套利深度的比例 | 0.5 |
| `sizing.max_order_notional_usd` | 单笔名义上限 | 500 |
| `inventory.scale_bps` / `floor_frac` | 库存阶梯（仓位超过上限的 `floor_frac` 后额外加价） | 10 / 0.5 |
| `inventory.max_paired_layers` | 最多同方向、已完整对冲的仓位层数 | 1 |
| `execution.premium_persist_sec` | 信号需持续多久才触发 | 0.3 |
| `execution.second_leg_latency_reserve_bps` | 可选的两腿串行额外净溢价保护 | 0.0 |
| `execution.*` | 滑点保护、超时、对账周期等 | 见配置文件 |
| `recorder.*` | 分钟数据采集器 | 开启，`logs/minutes.csv` |
| `logging.dashboard` / `logging.file` | 终端仪表盘；开启时日志写入文件 | 开启，`logs/engine.log` |

## 密钥配置（`.env`，仅实盘需要）

- **Entropy / tradexyz（Hyperliquid）** —— 在
  <https://app.hyperliquid.xyz/API> 创建 API（agent）钱包。`HL_PRIVATE_KEY`
  填 **agent 钱包私钥**，`HL_ACCOUNT_ADDRESS` 填主账户地址。当
  `--hedge tradexyz` 时两条腿默认共用该账户（内部自动共享 nonce 序列）；
  如需分开，设置 `HL_PRIVATE_KEY_XYZ` / `HL_ACCOUNT_ADDRESS_XYZ`。注意给
  所交易的各 dex 分别充入保证金。
- **Lighter** —— `LIGHTER_ACCOUNT_INDEX`、`LIGHTER_API_KEY_INDEX`、
  `LIGHTER_API_PRIVATE_KEY`，必须注册在与启动参数 `--hedge` **相同的部署**上
  （主网与 Robinhood 链是两套独立的账户和密钥——参见
  [lighter-python](https://github.com/elliottech/lighter-python)）。
- **Arcus** —— 填写 `ARCUS_ADDRESS`、`ARCUS_ACCOUNT_INDEX`、
  `ARCUS_API_KEY`，并在 `ARCUS_API_PRIVATE_KEY` 与
  `ARCUS_API_PRIVATE_KEY_FILE` 中二选一。这里使用 Ed25519 API 私钥，
  绝不能填写 Ethereum 主钱包私钥；`ARCUS_NETWORK` 默认是 `mainnet`。
  Arcus 测试网仅允许 `--record-only`，因为 Entropy 交易腿位于主网。

## 执行机制

- **Entropy 优先执行**：Entropy 使用短时受控限价单（关闭挂单模式时使用
  IOC）；只有确认 Entropy 成交后才发送对冲腿。挂单存活期间持续复核双腿净
  价差，任何无法确认的结果都会强制进入链上对账。
- Entropy IOC 撤单后只可在新盘口上重试一次，并必须满足绝对预算净收益线；
  多余价差只会动态放宽较难成交的 Entropy 腿，且不超过配置上限。
- **持续性闸门**（`premium_persist_sec`）：信号先"武装"，持续存在才触发，
  过滤单 tick 的假信号。
- **库存阶梯**：仓位超过上限的 `floor_frac` 后，同方向加仓需要线性递增的
  额外溢价，满仓时最高加 `scale_bps`。
- **平衡尘埃仓处理**：低于交易所最小下单量的配对仓位，既可补大，也可在
  有利方向按正常单笔金额穿过零点；穿零必须满足单独的绝对净收益门槛。
- **每日硬风控**：每日盈亏和成交量跨重启持久化；到达亏损线后可以停机或
  进入仅利润模式，紧急净敞口对冲始终保留。
- **净敞口对冲**：两腿成交不对等时立即用 reduce-only 单（带滑点保护）
  削减敞口，并每 `reconcile_sec` 与链上仓位对账。
- **私有成交快速确认**：Entropy IOC 会同时等待签名 REST 响应和 Hyperliquid
  `orderUpdates`。私有 WebSocket 一旦确认成交就立即发送对冲腿，REST 返回的
  精确成交均价放到对冲之后补齐；`second_leg_latency_reserve_bps` 是依据实盘
  数据校准的可选过滤器，而非默认提高门槛。
- **故障隔离**：被限频的交易所短暂暂停；交易所不可达（如例行维护）时暂停
  交易并每 `venue_probe_sec` 探测直至恢复；连续 `max_consecutive_errors`
  次执行异常则整体停机。
- **仅实盘**：没有模拟成交模式。`--record-only` 是唯一无风险的运行方式，
  其余都是真金白银。

## 目录结构

```
main.py                  入口（--record-only，默认即实盘）
entropy_arb/config.py    YAML + .env 配置契约与校验
entropy_arb/book.py      订单簿 + 含手续费的套利规模计算
entropy_arb/feeds.py     官方 HL ws + zkLighter ws 行情
entropy_arb/venue_hl.py  Hyperliquid dex 适配器（Entropy、tradexyz）
entropy_arb/venue_lighter.py  zkLighter 适配器（主网、Robinhood 链）
entropy_arb/venue_arcus.py  Arcus REST/WS + Ed25519 适配器
entropy_arb/engine.py    双交易所策略主循环
entropy_arb/dashboard.py Rich 终端仪表盘
entropy_arb/recorder.py  分钟级盘口数据采集
tools/analyze.py         minutes.csv -> 阈值建议
tests/                   python3 -m pytest tests/
```

## 已知风险

- **中枢填错就是亏钱策略。** 溢价中枢会漂移，请定期重新测量并保持
  `config.yaml` 与市场同步。
- **USDG 基差**（`lighter-rh`）：对冲腿以 USDG 计价，持续溢价中有
  一部分是稳定币本身的基差；midline 吸收其水平，但 USDG 的*变动*是真实盈亏。
- **资金费**：两个交易所、两套独立的资金费率，持仓成本未建模——仓位上限
  请设小一些。
- **薄盘口**：Entropy 深度可能很小；`take_fraction` 与名义上限控制单笔规模，
  但部分成交后对冲腿的滑点是真实存在的。
- **交易时段**：股票类永续（如 SNDK）盘后各所预言机行为不同，建议加宽带宽
  或避开盘后。
- **单腿风险**：一条腿成交后另一条可能失败。机器人会自动对冲并对账，但
  仍需人工关注。

风险自负。本软件直接操作真实资金，本文档不构成任何投资建议。请从最小的
仓位上限开始。

## 开源协议

[MIT](LICENSE)
## 每小时未成交原因统计

### 强平与异常持仓保护

GTC 提交响应中的 `filled` 不再无条件视为整单结束：即时成交量未达到请求数量时，使用响应订单 ID（缺失时仅尝试唯一匹配）核验，并撤销尚未关闭的余单。撤单期间新增成交以最终累计量为准；低于已知成交量的旧快照不作为完成确认。无法确认撤单时保持 pending 暂停，无法确认订单身份时进入持久化 `POSITION-REVIEW`。全部即时成交仍保留原快速对冲路径。此修复不会清除现有锁，也不会自动补算历史漏记成交；部署前仍须人工核对旧挂单及两边真实仓位。

实盘 Hyperliquid 账户连接额外订阅 `userFills` 和 `userEvents`。收到本账户的实时强平事件，或本策略币种从本次连接生命周期开始后的强平成交（包括重连快照补发），会进入 `POSITION-REVIEW`。启动前旧强平成交不会重复触发。账户级强平通知可能来自同账户其他市场，也会保守暂停。普通成交不会仅因缺少机器人订单 ID 被判作异常。

运行中持仓对账差额超过 `net_tolerance_base` 时，同样锁定；启动时正常持仓基线与原有成交后对账宽限保持不变。锁定后：暂停新策略及自动净敞口对冲；已发出的请求不能撤回，正在管理的限价单仍按原有撤单/确认流程处理；继续采集行情和账户持仓快照，但不自动把异常快照覆盖成本地仓位，也不声称已经归零。若第一腿已成交而检测到该锁，不再根据可能过期的库存计算发送新对冲，必须人工处理真实敞口。

日志标记为 `[POSITION REVIEW]`、`[POSITION REVIEW SNAPSHOT]`；锁文件为 `risk.daily_risk_state_file` 配置路径加 `.position-review` 后缀（未配置时为 `logs/daily_risk.json.position-review`）。UTC 跨日与进程重启均不解除；锁文件存在时启动仍禁止实盘下单。若写锁失败，当前进程仍保持暂停并报错，切勿直接重启实盘。

恢复前必须停止机器人，核对两边真实仓位、所有挂单与成交历史（包括强平），人工处理敞口并确认保证金情况。完成这些核验后才能归档对应锁文件并重新启动建立账户基线，不应只为恢复交易删除锁。此功能不调整保证金/杠杆，不保证事件流无延迟、不保证避免强平，也不包含 Arcus 价格边界修复。

`[VOLUME STATS]` 的 `directions` 按买/卖 Entropy 分开记录最终配对结果、撤单原因、预期/实际配对价差收益，以及各段耗时、盘口距离、对冲耗时/报价漂移的样本数、中位数、P95。按执行结束所在 UTC 小时归属；实际价格未确认的笔数单列，不当作零收益。收益口径为两腿配对价差扣交易手续费，不包含资金费、库存浮盈亏或后续紧急对冲。预期收益仅累加成功配对的实际匹配数量，与零成交的纸面预期分开。每方向每指标最多保留最近 10000 个样本计算分位数；结果次数和金额仍为当小时全量。`[PAIR DIAG]` 保留逐笔关联结果。

提交耗时新增 `sign_ms`（订单构建/签名）和 `request_ms`（传输至响应，包含服务端处理，不是纯网络 RTT）。原有 `submit_ms` 为两者之和；无需新增配置。

订单诊断日志：`[ENTROPY QUOTE]` 记录最终限价、提交前可成交盘口、价格距离（正值允许穿过盘口，负值为被动挂价）和行情年龄；`[ENTROPY DIAG]` 记录提交、挂单、撤单确认、总耗时（毫秒）。提交包括签名和请求响应，挂单从确认/发现订单后开始，不是精确的交易所驻留时长。撤单原因为 `ttl`、`partial-fill` 或 `edge-or-book`；后者细分为 `shutdown`、`risk`、`venue-down`、`stale-book`、`empty-book`、`insufficient-edge`。仅增加诊断，不改变保护条件。

实盘界面显示当前 UTC 小时统计（启动后的不足一小时也显示）。每秒对买、卖两个方向各采样一次，按首个阻塞条件归类：价差不足、库存限制、接口/行情暂停、风控暂停、冷却/等待、深度/最小单量。库存限制包括持仓/层数上限，以及库存附加门槛单独挡住的机会；接口暂停包括行情过期、未就绪、限频和撤单状态待确认。

百分比以阻塞采样总数为分母，可执行采样另列。这是方向时间占比，不是错失订单数，不保证增加容量后能成交。实际配对下单单独统计完整成交、部分成交、零成交、异常/未确认及占比；内部重试不重复计数，紧急净敞口对冲不计入配对下单统计。统计不改变策略条件或风控。

每小时结束写入日志，停止时写入当小时部分统计；重启后统计重新开始，不补历史数据、不重置风控。查看历史：

```bash
grep '\[VOLUME STATS\]' logs/engine.log | tail -n 24
```
