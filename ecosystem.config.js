module.exports = {
  apps: [
    {
      name: "Kernel",
      script: "./launch.sh",
      cwd: __dirname,
      watch: ["./flag/"],
      watch_delay: 1,
      ignore_watch: ["node_modules", ".git", ".venv", "__pycache__", ".bak"],
      log_file: "/tmp/kernel.log",
      error_file: "/tmp/error.log",
      out_file: "/tmp/out.log",
      log_date_format: "YYYY-MM-DDTHH:mm:ssZ",
      merge_logs: true,
      time: true,
      autorestart: true,
      exec_interpreter: "bash",
      exec_mode: "fork",
      instances: 1,
    },
  ],
};
