defmodule FolioTest do
  use Folio.DataCase
  # doctest Folio
  alias Folio.Schemas.Dog
  alias Folio.Schemas.Superhero
  alias Folio.TestRepo
  import Ecto.Query

  @bruce_banner %{first_name: "Bruce", last_name: "Banner", alternate_selves: 1}
  @natasha_romanoff %{first_name: "Natasha", last_name: "Romanoff", alternate_selves: 0}
  @bruce_wayne %{first_name: "Bruce", last_name: "Wayne", alternate_selves: 1}
  @thor %{first_name: "Thor", last_name: "Odinson", alternate_selves: 0}
  @tony_stark %{first_name: "Tony", last_name: "Stark", alternate_selves: 1}
  @wanda_maximoff %{first_name: "Wanda", last_name: "Maximoff", alternate_selves: 1}

  @people [
    @bruce_banner,
    @natasha_romanoff,
    @bruce_wayne,
    @thor,
    @tony_stark,
    @wanda_maximoff
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

  describe "mode - cursor" do
    test "cursor-based pagination - defaults", %{people: people} do
      stream = Folio.page(TestRepo, Superhero, mode: :cursor)
      assert [results] = get_results(stream)
      assert results == people
    end

    test "cursor-based pagination - batch_size option", %{people: people} do
      stream = Folio.page(TestRepo, Superhero, mode: :cursor, batch_size: 2)
      assert get_results(stream) == Enum.chunk_every(people, 2)
    end

    test "cursor-based pagination - order_by option", %{people: people} do
      stream = Folio.page(TestRepo, Superhero, mode: :cursor, order_by: :last_name)
      assert [results] = get_results(stream)
      assert results == Enum.sort_by(people, & &1.last_name)
    end

    test "cursor-based pagination - order_by option - desc direction", %{people: people} do
      stream = Folio.page(TestRepo, Superhero, mode: :cursor, order_by: {:desc, :last_name})

      sorted_by_last_name = Enum.sort_by(people, & &1.last_name, :desc)
      assert [results] = get_results(stream)
      assert results == sorted_by_last_name

      stream = Folio.page(TestRepo, Superhero, mode: :cursor, order_by: [desc: :last_name])

      assert [results] = get_results(stream)
      assert results == sorted_by_last_name
    end

    test "cursor-based pagination - order_by option - 2 cursors 2 directions" do
      stream =
        Folio.page(TestRepo, Superhero,
          mode: :cursor,
          order_by: [:first_name, desc: :last_name],
          batch_size: 1
        )

      assert get_results(stream) == [
               [@bruce_wayne],
               [@bruce_banner],
               [@natasha_romanoff],
               [@thor],
               [@tony_stark],
               [@wanda_maximoff]
             ]
    end

    test "cursor-based pagination - order_by option - cursor specified" do
      stream =
        Folio.page(TestRepo, Superhero,
          mode: :cursor,
          order_by: [:first_name],
          cursor: "Tony"
        )

      assert get_results(stream) == [[@tony_stark, @wanda_maximoff]]
    end

    test "not specifying cursor when table does not have primary key raises" do
      TestRepo.insert_all(Dog, [%{breed: "Bull Terrier", name: "Fido"}])

      assert_raise Folio.FolioError, fn ->
        Folio.page(TestRepo, Dog, mode: :cursor)
      end
    end

    test "select multiple fields as struct", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          Map.drop(%Superhero{first_name: person.first_name}, [:__meta__])
        end)

      stream =
        Folio.page(TestRepo, Superhero, mode: :cursor, select: [:first_name], select_as_map: false)

      [results] = Enum.to_list(stream)
      assert Enum.map(results, &Map.drop(&1, [:__meta__])) == first_names
    end

    test "select multiple fields as map (default)", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          %{first_name: person.first_name}
        end)

      stream = Folio.page(TestRepo, Superhero, mode: :cursor, select: [:first_name])

      [results] = Enum.to_list(stream)
      assert results == first_names
    end

    test "select single field", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          person.first_name
        end)

      stream = Folio.page(TestRepo, Superhero, mode: :cursor, select: :first_name)

      [results] = Enum.to_list(stream)
      assert results == first_names
    end

    test "with query instead of schema", %{people: people} do
      stream = Folio.page(TestRepo, from(s in Superhero), mode: :cursor, order_by: :id)
      assert [results] = get_results(stream)
      assert results == people
    end
  end

  describe "mode - offset" do
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

    test "not specifying cursor when table does not have primary key raises" do
      TestRepo.insert_all(Dog, [%{breed: "Bull Terrier", name: "Fido"}])

      assert_raise Folio.FolioError, fn ->
        Folio.page(TestRepo, Dog, mode: :offset)
      end
    end

    test "select multiple fields as struct", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          Map.drop(%Superhero{first_name: person.first_name}, [:__meta__])
        end)

      stream =
        Folio.page(TestRepo, Superhero, mode: :offset, select: [:first_name], select_as_map: false)

      [results] = Enum.to_list(stream)
      assert Enum.map(results, &Map.drop(&1, [:__meta__])) == first_names
    end

    test "select multiple fields as map (default)", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          %{first_name: person.first_name}
        end)

      stream = Folio.page(TestRepo, Superhero, mode: :offset, select: [:first_name])

      [results] = Enum.to_list(stream)
      assert results == first_names
    end

    test "select single field", %{people: people} do
      first_names =
        Enum.map(people, fn person ->
          person.first_name
        end)

      stream = Folio.page(TestRepo, Superhero, mode: :offset, select: :first_name)

      [results] = Enum.to_list(stream)
      assert results == first_names
    end

    test "with query instead of schema", %{people: people} do
      stream = Folio.page(TestRepo, from(s in Superhero), mode: :offset, order_by: :id)
      assert [results] = get_results(stream)
      assert results == people
    end
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
