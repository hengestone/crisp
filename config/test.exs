import Config

config :logger, :console,
  format: {Crisp.DevLogger, :format},
  metadata: [:module, :line, :pid, :mfa, :function],
  level: :debug
