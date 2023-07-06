import Config

config :logger, :console,
  format: "[$level] $time $metadata\n       $message\n",
  metadata: [:request_id, :mfa],
  level: :error

config :logger, :console_log_file,
  format: {Crisp.DevLogger, :format},
  metadata: [:module, :line, :pid, :mfa],
  level: :info,
  path: "log/console.log"

config :logger,
  backends: [:console, {LoggerFileBackend, :console_log_file}]
