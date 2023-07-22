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

    sorted_by_id =
      Superhero
      |> order_by(:id)
      |> select([sh], map(sh, [:first_name, :last_name, :alternate_selves]))
      |> TestRepo.all()

    %{people: sorted_by_id}
  end

  test "offset based pagination - defaults", %{people: people} do
    stream = Folio.page(TestRepo, Superhero, mode: :offset)
    assert [results] = get_results(stream)
    assert results == people
  end

  test "offset based pagination - batch_size option", %{people: people} do
    stream = Folio.page(TestRepo, Superhero, mode: :offset, batch_size: 2)
    assert get_results(stream) == Enum.chunk_every(people, 2)
  end

  test "offset based pagination - offset option", %{people: people} do
    stream = Folio.page(TestRepo, Superhero, mode: :offset, offset: length(people) - 1)
    assert [results] = get_results(stream)
    assert results == [List.last(people)]
  end

  test "offset based pagination - order_by option", %{people: people} do
    stream = Folio.page(TestRepo, Superhero, mode: :offset, order_by: :last_name)
    assert [results] = get_results(stream)
    assert results == Enum.sort_by(people, & &1.last_name)
  end

  # For easier comparison
  defp get_results(stream) do
    Enum.map(stream, fn page ->
      Enum.map(page, fn result ->
        result |> Map.from_struct() |> Map.drop([:id, :__meta__])
      end)
    end)
  end
end
