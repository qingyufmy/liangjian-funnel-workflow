module.exports = {
  apps: [
    {
      name: "liangjian-funnel-console",
      cwd: __dirname,
      script: "dist/server/index.js",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "1G",
      kill_timeout: 15000,
      listen_timeout: 10000,
      merge_logs: true,
      time: true,
      env: {
        NODE_ENV: "production",
        HOST: "127.0.0.1",
        PORT: "3210",
        TZ: "Asia/Shanghai",
        LIANGJIAN_PYTHON_BIN: ".venv/bin/python",
        LIANGJIAN_WEB_DIST: "dist/web",
        LIANGJIAN_SCHEDULER_ENABLED: "true",
      },
    },
  ],
};
