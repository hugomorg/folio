defmodule Folio.TestRepo do
  use Ecto.Repo,
    otp_app: :folio,
    adapter: Ecto.Adapters.Postgres
end
