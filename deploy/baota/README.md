# 宝塔 Node 项目配置

在宝塔“Node 项目”中使用以下参数：

- 项目目录：仓库根目录
- Node 版本：20 LTS 或 22 LTS
- 包管理器：npm
- 安装命令：`npm ci`
- 构建命令：`npm run build`
- 启动文件：`dist/server/index.js`
- 运行用户：独立的非 root 用户
- 实例数：1（禁止 cluster 多实例）
- 监听：`127.0.0.1:3210`

Python 3.11 虚拟环境仍放在项目根目录 `.venv`：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install .
.venv/bin/python -m liangjian_funnel doctor
```

将 `.env.example` 复制为 `.env` 并只在服务器填写密钥。Node 控制台环境变量可在宝塔项目设置中填写；建议至少设置：

```text
NODE_ENV=production
HOST=127.0.0.1
PORT=3210
TZ=Asia/Shanghai
LIANGJIAN_PYTHON_BIN=.venv/bin/python
LIANGJIAN_WEB_DIST=dist/web
LIANGJIAN_SCHEDULER_ENABLED=true
LIANGJIAN_DASHBOARD_TOKEN=<随机长令牌>
```

反向代理使用 [nginx.conf.example](nginx.conf.example)。若只允许自己访问，还应在宝塔网站设置 IP 白名单、Basic Auth 或 VPN；应用令牌是额外保护，不代替服务器访问控制。

首次启动前创建并授权四个持久化目录：

```bash
mkdir -p outputs/node outputs/scheduler state storage cache
```

备份必须同时包含 `.env`（单独加密）、`state/workflow.sqlite3`、`storage/` 和 `outputs/`。更新时先停止 Node 项目，备份状态，再执行 `git pull && npm ci && npm run build && .venv/bin/python -m pip install .`，最后由宝塔启动项目。
