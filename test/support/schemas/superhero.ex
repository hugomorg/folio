defmodule Folio.Schemas.Superhero do
  use Ecto.Schema

  schema "superheroes" do
    field(:first_name, :string)
    field(:last_name, :string)
    field(:alternate_selves, :integer)
  end
end
