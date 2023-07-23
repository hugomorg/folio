defmodule Folio.DataCase do
  use ExUnit.CaseTemplate

  alias Ecto.Adapters.SQL.Sandbox

  using do
    quote do
      import Ecto
      import Ecto.Changeset
      import Ecto.Query

      alias Folio.TestRepo
      alias Folio.Schemas.Person
    end
  end

  setup tags do
    :ok = Sandbox.checkout(Folio.TestRepo)

    unless tags[:async] do
      Sandbox.mode(Folio.TestRepo, {:shared, self()})
    end

    :ok
  end
end
