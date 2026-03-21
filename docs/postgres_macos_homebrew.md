# macOS（Homebrew）安装/启动 PostgreSQL 与最小验证步骤

本文档提供一套在 macOS 上使用 Homebrew 安装并启动 PostgreSQL 的最小流程，并包含最小验证步骤：`psql` 连接、创建数据库（用于后续 TPC-DS benchmark）。

## 0. 前置条件

- 已安装 Homebrew：`brew --version`

## 1. 安装 PostgreSQL

查看可用版本（可选）：

```bash
brew search postgresql
```

推荐安装一个固定大版本（示例为 16；如你环境中没有 16，请按 `brew search` 结果替换为可用版本）：

```bash
brew install postgresql@16
```

确认客户端可用：

```bash
psql --version
```

## 2. 启动/停止 PostgreSQL 服务

使用 Homebrew Services 后台启动：

```bash
brew services start postgresql@16
```

查看服务状态：

```bash
brew services list | grep postgres
```

停止/重启（可选）：

```bash
brew services stop postgresql@16
brew services restart postgresql@16
```

如果你不想用后台服务，也可以用 `pg_ctl` 手动启动（不推荐作为长期方案）：

```bash
pg_ctl -D "$(brew --prefix)/var/postgresql@16" start
```

## 3. 最小验证：psql 连接与基础 SQL

检查是否就绪（可选）：

```bash
pg_isready
```

尝试连接默认维护库 `postgres` 并执行一条 SQL：

```bash
psql -d postgres -c "SELECT version();"
```

若报错 `role \"<你的用户名>\" does not exist`，说明本机角色尚未创建。可创建与当前系统用户同名的角色（只用于本机开发，最简单）：

```bash
createuser -s "$(whoami)"
psql -d postgres -c "SELECT 1;"
```

## 4. 创建 benchmark 数据库与专用用户（推荐）

为后续 benchmark 建议创建一个独立的数据库与用户，避免使用系统同名用户做实验。

先用本机管理员连接（通常是当前系统用户）进入 `postgres`：

```bash
psql -d postgres
```

在 `psql` 里执行（把密码替换成你自己的；示例使用 `bench/bench`）：

```sql
CREATE ROLE bench WITH LOGIN PASSWORD 'bench';
CREATE DATABASE tpcds_sf1 OWNER bench;
GRANT ALL PRIVILEGES ON DATABASE tpcds_sf1 TO bench;
```

退出 `psql`：

```text
\q
```

## 5. 最小验证：使用新用户连接并建库成功

用 DSN 直连并执行一条 SQL（这一步同时验证账号/密码/数据库名/端口都正确）：

```bash
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -c "SELECT current_user, current_database();"
```

你也可以在 `tpcds_sf1` 内再创建一个用于后续脚本的测试数据库对象（可选）：

```bash
psql "postgresql://bench:bench@localhost:5432/tpcds_sf1" -c "CREATE SCHEMA IF NOT EXISTS public;"
```

## 6. 给本仓库使用的 DSN 示例

本仓库通过 CLI 参数 `--dsn` 传入连接串（见 [validator_cli.py](../cli/validator_cli.py)）。

示例：

```text
postgresql://bench:bench@localhost:5432/tpcds_sf1
```

## 7. 常见问题（简要）

- 端口冲突：默认端口是 5432；如你本机已有 PostgreSQL/其他服务占用，需停止旧服务或改端口后再连接。
- 服务启动失败：先用 `brew services list` 确认状态，再查看日志（Homebrew 版本不同日志位置可能不同）。
- 连接提示要求密码：如果 DSN 包含 `user:password@`，会使用密码认证；若你希望免密，请使用本机同名用户并按 Homebrew 默认鉴权方式连接。
