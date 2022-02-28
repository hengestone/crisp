import Config

config :crisp,
  namespace: "crisp_#{Mix.env()}"

config :crisp, :redis,
  host: "localhost",
  port: 6379,
  name: :crisp

config :crisp, :web_handler, asset_path: "priv/web/"

import_config "#{Mix.env()}.exs"
