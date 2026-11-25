# Quant CTA 部署

Quant CTA 使用 Python、SQLite 和 Playwright，优先使用 Mac 上已安装的 Google Chrome。面板在 `127.0.0.1:8766` 监听，每五分钟采集一次。SQLite、配置、日志和安装版本保存在 `~/Library/Application Support/QuantCTA`。

## 在 Mac mini 上检查环境

以运行服务的用户登录 Mac，先运行以下命令。

```sh
hostname
uname -m
sw_vers
python3 --version
node --version
npm --version
lsof -nP -iTCP:8766 -sTCP:LISTEN
df -h "$HOME"
```

需要 Python 3.9 或更新版本，以及 Node.js 22 或更新版本。若系统没有安装 Google Chrome，安装器会下载 Chromium Headless Shell。`lsof` 首次部署时无输出表示未发现该端口上的监听进程。

## 安装与验证

将项目文件复制到 Mac 上的同一目录，从该目录运行安装器。以已登录桌面的用户安装 `net.olven.quant`。用户登录后自动运行，注销期间暂停。具备管理员认证时也可安装系统 LaunchDaemon。服务启动后 30 秒内未返回 HTTP 200 时，安装器恢复上一版配置。

```sh
python3 install_macos.py --user-agent
launchctl print "gui/$(id -u)/net.olven.quant"
curl --fail http://127.0.0.1:8766/healthz
```

进入安装器输出的版本目录后，用下列命令检查数据源与 Binance 行情连接。使用自定义配置部署时，附加相同的 `--config /path/to/config.json`。

```sh
python3 app.py --check
```

`--check` 检查数据源，数据库无写入。若需在启动常驻服务前手动写入一次采样，可运行 `python3 app.py --once`。上线后检查日志和面板最近更新时间。

```sh
quant_data="$HOME/Library/Application Support/QuantCTA"
tail -n 60 "$quant_data/service.out.log" "$quant_data/service.err.log"
```

重新运行安装器升级。SQLite 和配置保留。默认配置位置为 `~/Library/Application Support/QuantCTA/config.json`。也可通过 `python3 install_macos.py --config /path/to/config.json` 指定已有配置，后续升级会沿用已登记的路径。

## 采集和模拟规则

默认对 `symbols.json` 中原站内置的 28 个币采集 5m 至周线的因子矩阵、综合概率、分类得分和页面提供的命中率及 IC。自选范围会影响原站的横截面因子。原站用公开市场数据在浏览器计算加密因子，仓库公开可读，未发现开源许可证。采集器读取原站展示的数据，不依赖原站代码副本。

各币保留独立 1,000 USDT 模拟账户。默认采用原站综合概率页的历史 IC 加权 5m 多头概率，≥65% 做多、≤35% 做空、中间平仓，反向时先平再开。原站阈值用于标记信号强弱，开平仓是本模拟盘的规则。通常使用可交易的 Binance USDT 永续，SHIB 使用 `1000SHIBUSDT`。部署时 TON 永续处于结算，现货也没有活动盘口，因此仅用实时更新的 Binance 参考标记价模拟成交和估值，不计资金费。价格失去时效时暂停该币成交。胜率按已平仓交易的净损益计算。手续费和滑点分别按 5 和 2 个基点估算，资金费使用可获取的已结算记录，强平按采样时点及简化的维持保证金率估算。

面板可切换三套固定阈值：35%/65%、40%/60%、45%/55%。三套共用每轮采集的信号、报价及已结算资金费记录，各自独立记录仓位与盈亏。原有 `quant.sqlite3` 继续存储 35%/65% 账本和原始因子快照，新策略分别写入 `quant-40-60.sqlite3` 和 `quant-45-55.sqlite3`，从启用后的首轮开始。备份时保留这三个数据库。

如果所在网络需要通过代理访问 Binance，可在配置中添加 `market_proxy`，值为已有 HTTP 代理的地址。代理配置和模拟规则可在下一轮采集前修改。将 `rule.enabled` 设为 `false` 可暂停新的信号交易。

~~~json
{
  "rule": {"enabled": true, "mode": "probability", "period": "5m", "long_at": 0.65, "short_at": 0.35},
  "fee_bps": 5,
  "slippage_bps": 2,
  "funding": true
}
~~~

面板的历史截止时间使用北京时间。原站因子历史从安装后开始积累，每轮保留采集时间、最新 K 线时间、原站构建版本、覆盖率和错误。采集浏览器在采样之间保持运行。首次加载全部币种的周期数据需要几分钟，缺失字段在状态区显示。

## Cloudflare 配置

先从 Mac 本机确认 `http://127.0.0.1:8766/` 可以访问。若需要成员登录，在 **Zero Trust → Access controls → Applications** 下创建自托管应用，选择面板域名并添加允许访问的用户策略。

1. 在 Cloudflare **Networking → Tunnels** 中选择本机 Cloudflare Tunnel。
2. 打开 **Routes → Add route → Published application**，填写下表并保存。

| 字段 | 值 |
| --- | --- |
| Subdomain | `quant` |
| Domain | 选择你托管的域名 |
| Path | 留空 |
| Service URL | `http://127.0.0.1:8766` |

Cloudflare 托管完整 DNS 时，保存路由会自动创建 DNS 记录。打开面板域名检查 HTTPS 和页面数据。

回环地址只在 connector 与 Quant CTA 同机时有效。同一 Tunnel 的各个 connector 都可能接到请求。若现有 Tunnel 在另一台机器，或有多个无法访问这个回环服务的 connector，可让 Mac 单独运行一个 Tunnel。有受控内网连接时，也可以让各个现有 connector 共用一个能到达的私有 origin 地址。

[Cloudflare Tunnel 创建与发布路由](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/) · [Cloudflare Access 应用设置](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/self-hosted-public-app/) · [Cloudflare Tunnel 副本行为](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/configure-tunnels/tunnel-availability/) · [Playwright 浏览器安装](https://playwright.dev/docs/browsers)
