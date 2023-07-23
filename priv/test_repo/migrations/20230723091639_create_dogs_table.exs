defmodule Folio.TestRepo.Migrations.CreateDogsTable do
  use Ecto.Migration

  def change do
    create table :dogs, primary_key: false do
      add :name, :string
      add :breed, :string
    end
  end
end
