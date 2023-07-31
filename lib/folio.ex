defmodule Folio do
  @moduledoc """
  Documentation for `Folio`.
  """

  @doc """
  TBC
  """

  import Ecto.Query

  defmodule FolioError do
    defexception [:message]
  end

  def page(schema, repo, opts \\ [])

  def page(schema, repo, opts) when is_list(opts) do
    page(schema, repo, Map.new(opts))
  end

  def page(schema, repo, opts) do
    create_stream(schema, repo, build_opts(schema, repo, opts))
  end

  defp build_opts(schema, _repo, opts = %{mode: :offset}) do
    batch_size = Map.get(opts, :batch_size, 100)
    offset = Map.get(opts, :offset, 0)

    order_by =
      Map.get_lazy(opts, :order_by, fn ->
        get_primary_key!(schema)
      end)

    fields_to_select = Map.get(opts, :select)
    select_as_map = Map.get(opts, :select_as_map, true)

    %{
      batch_size: batch_size,
      offset: offset,
      mode: :offset,
      order_by: order_by,
      fields_to_select: fields_to_select,
      select_as_map: select_as_map
    }
  end

  defp build_opts(schema, repo, opts = %{mode: :cursor}) do
    batch_size = Map.get(opts, :batch_size, 100)

    order_by =
      Map.get_lazy(opts, :order_by, fn ->
        get_primary_key!(schema)
      end)

    cursor =
      Map.get_lazy(opts, :cursor, fn ->
        get_default_cursor(repo, schema, order_by)
      end)

    order_by = order_by |> normalise_order_by() |> List.wrap()
    cursor = List.wrap(cursor)

    fields_to_select = Map.get(opts, :select)
    select_as_map = Map.get(opts, :select_as_map, true)

    # Track that this is the first request so that we include the initial cursor
    # but subsequent pages should fetch after the cursor (exclusive)
    %{
      batch_size: batch_size,
      mode: :cursor,
      order_by: order_by,
      cursor: cursor,
      first: true,
      fields_to_select: fields_to_select,
      select_as_map: select_as_map
    }
  end

  defp get_primary_key!(%Ecto.Query{}) do
    raise __MODULE__.FolioError,
      message: "Please specify an order_by field when using a query"
  end

  defp get_primary_key!(schema) do
    case schema.__schema__(:primary_key) do
      [] ->
        raise __MODULE__.FolioError,
          message:
            "Please specify an order_by field - your schema #{inspect(schema)}" <>
              " doesn't have a primary key to fall back on"

      pk ->
        pk
    end
  end

  defp normalise_order_by(order_by) when is_list(order_by) do
    Enum.map(order_by, &normalise_order_by/1)
  end

  defp normalise_order_by({dir, field}) do
    {dir, field}
  end

  defp normalise_order_by(field) do
    {:asc, field}
  end

  defp create_stream(schema, repo, initial_params) do
    Stream.unfold(initial_params, &run_stream(schema, repo, &1))
  end

  defp run_stream(_schema, _repo, :done), do: nil

  defp run_stream(schema, repo, params) do
    schema |> build_query(params) |> repo.all |> handle_results(params)
  end

  defp handle_results([], _params), do: nil

  defp handle_results(results, params) do
    if length(results) < params.batch_size do
      {results, :done}
    else
      next_params = build_next_params(results, params)
      {results, next_params}
    end
  end

  defp build_query(schema, %{
         mode: :offset,
         batch_size: batch_size,
         order_by: order_by,
         offset: offset,
         fields_to_select: fields_to_select,
         select_as_map: select_as_map
       }) do
    schema
    |> limit(^batch_size)
    |> offset(^offset)
    |> order_by(^order_by)
    |> maybe_select_fields(fields_to_select, select_as_map)
  end

  defp build_query(
         schema,
         opts = %{
           mode: :cursor,
           batch_size: batch_size,
           order_by: order_by,
           fields_to_select: fields_to_select,
           select_as_map: select_as_map
         }
       ) do
    schema
    |> limit(^batch_size)
    |> build_cursor_where_query(opts)
    |> maybe_select_fields(fields_to_select, select_as_map)
    |> order_by(^order_by)
  end

  defp maybe_select_fields(query, nil, _), do: query
  defp maybe_select_fields(query, [], _), do: query

  defp maybe_select_fields(query, fields_to_select, true) when is_list(fields_to_select) do
    select(query, [el], map(el, ^fields_to_select))
  end

  defp maybe_select_fields(query, fields_to_select, false) when is_list(fields_to_select) do
    select(query, ^fields_to_select)
  end

  defp maybe_select_fields(query, field, _) do
    select(query, [el], field(el, ^field))
  end

  # The main idea here is that we want to get the next results after
  # the current cursor, but if the first n fields match then we want to try
  # the next cursor field, and if that is equal to any given row, then
  # we try the next field etc
  defp build_cursor_where_query(
         query,
         opts = %{
           cursor: [_ | _] = cursor,
           order_by: [_ | _] = order_by
         }
       ) do
    {_, dynamic_query} =
      order_by
      |> Enum.zip(cursor)
      |> Enum.reduce({[], dynamic(true)}, &build_multi_cursor_query(&1, &2, opts[:first]))

    where(query, ^dynamic_query)
  end

  defp build_multi_cursor_query({field, cursor}, {prev_cursor_field_pairs, dynamic_query}, first) do
    matching_fields_query =
      Enum.reduce(prev_cursor_field_pairs, dynamic(true), fn {{_direction, field}, cursor},
                                                             dynamic_query ->
        dynamic([el], ^dynamic_query and field(el, ^field) == ^cursor)
      end)

    latest_field_comparison = field_cursor_comparison(dynamic(true), field, cursor, first)

    this_field_comparison = dynamic(^matching_fields_query and ^latest_field_comparison)

    case prev_cursor_field_pairs do
      [] ->
        {[{field, cursor} | prev_cursor_field_pairs], dynamic(^this_field_comparison)}

      _ ->
        {[{field, cursor} | prev_cursor_field_pairs],
         dynamic(^dynamic_query or ^this_field_comparison)}
    end
  end

  defp field_cursor_comparison(d, {:asc, field}, cursor, true) do
    dynamic([el], ^d and field(el, ^field) >= ^cursor)
  end

  defp field_cursor_comparison(d, {:asc, field}, cursor, _first) do
    dynamic([el], ^d and field(el, ^field) > ^cursor)
  end

  defp field_cursor_comparison(d, {:desc, field}, cursor, true) do
    dynamic([el], ^d and field(el, ^field) <= ^cursor)
  end

  defp field_cursor_comparison(d, {:desc, field}, cursor, _first) do
    dynamic([el], ^d and field(el, ^field) < ^cursor)
  end

  defp build_next_params(_results, params = %{mode: :offset}) do
    Map.update!(params, :offset, &(&1 + params.batch_size))
  end

  defp build_next_params(results, params = %{mode: :cursor, first: true}) do
    build_next_params(results, Map.delete(params, :first))
  end

  defp build_next_params(results, params = %{mode: :cursor, order_by: order_by}) do
    next_cursor = results |> List.last() |> next_cursor(order_by)
    %{params | cursor: next_cursor}
  end

  defp next_cursor(last, order_by) when is_list(order_by) do
    Enum.map(order_by, fn order_by_field ->
      next_cursor(last, order_by_field)
    end)
  end

  defp next_cursor(last, {_direction, order_by}) do
    next_cursor(last, order_by)
  end

  defp next_cursor(last, order_by) do
    Map.fetch!(last, order_by)
  end

  defp get_default_cursor(repo, schema, {:asc, cursor_field}) do
    schema |> select([el], min(field(el, ^cursor_field))) |> repo.one!
  end

  defp get_default_cursor(repo, schema, {:desc, cursor_field}) do
    schema |> select([el], max(field(el, ^cursor_field))) |> repo.one!
  end

  defp get_default_cursor(repo, schema, cursor_fields) do
    schema |> order_by(^cursor_fields) |> limit(1) |> repo.one! |> next_cursor(cursor_fields)
  end
end
