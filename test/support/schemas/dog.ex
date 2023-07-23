defmodule Folio.Schemas.Dog do
  use Ecto.Schema

  @primary_key false
  schema "dogs" do
    field(:name, :string)
    field(:breed, :string)
  end
end
