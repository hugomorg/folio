defmodule Folio.TestRepo.Migrations.CreateSuperheroesTable do
  use Ecto.Migration

  def change do
    create table :superheroes do
      add :first_name, :string
      add :last_name, :string
      add :alternate_selves, :integer
    end
  end
end
