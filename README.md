# Quant CTA

把 CTA 多因子看板变成可以回看的模拟盘。服务每五分钟采集原站内置币种的因子快照，用 Binance 公开报价给独立的模拟账户估值，记录仓位、净值、盈亏和胜率。所有交易都在本地 SQLite 中模拟。

[部署与维护说明](DEPLOY.md)涵盖 macOS 后台服务、SQLite 数据目录和 Cloudflare Tunnel。原站和公开行情的使用方式见部署说明。
