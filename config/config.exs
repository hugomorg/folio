import Config

config :folio, ecto_repos: [Folio.TestRepo]

config :folio, Folio.TestRepo,
  username: "postgres",
  password: "postgres",
  database: "folio_test",
  hostname: "localhost",
  port: 5432,
  pool: Ecto.Adapters.SQL.Sandbox
