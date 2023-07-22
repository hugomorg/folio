defmodule FolioTest do
  use ExUnit.Case
  # doctest Folio
  alias Folio.Schemas.Superhero
  alias Folio.TestRepo
  import Ecto.Query

  @people [
    %{first_name: "Bruce", last_name: "Banner", alternate_selves: 1},
    %{first_name: "Bruce", last_name: "Wayne", alternate_selves: 1},
    %{first_name: "Natasha", last_name: "Romanoff", alternate_selves: 0},
    %{first_name: "Thor", last_name: "Odinson", alternate_selves: 0},
    %{first_name: "Tony", last_name: "Stark", alternate_selves: 1},
    %{first_name: "Wanda", last_name: "Maximoff", alternate_selves: 1}
  ]

  setup do
    TestRepo.insert_all(Superhero, @people)
    sorted_by_id = Superhero |> order_by(:id) |> TestRepo.all()
    %{people: sorted_by_id}
  end

  test "offset based pagination - defaults", %{people: people} do
    assert stream = Folio.page(TestRepo, Superhero, mode: :offset)
    assert Enum.to_list(stream) == [people]
  end
end
