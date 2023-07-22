defmodule Folio do
  @moduledoc """
  Documentation for `Folio`.
  """

  @doc """
  TBC
  """

  import Ecto.Query

  def page(repo, schema, opts \\ [])

  def page(repo, schema, opts) when is_list(opts) do
    page(repo, schema, Map.new(opts))
  end

  def page(repo, schema, opts) do
    create_stream(repo, schema, build_opts(repo, schema, opts))
  end

  defp build_opts(_repo, _schema, opts = %{mode: :offset}) do
    batch_size = Map.get(opts, :batch_size, 100)
    offset = Map.get(opts, :offset, 0)
    order_by = Map.get(opts, :order_by, :id)
    %{batch_size: batch_size, offset: offset, mode: :offset, order_by: order_by}
  end

  defp build_opts(repo, schema, opts = %{mode: :cursor}) do
    batch_size = Map.get(opts, :batch_size, 100)
    order_by = Map.get(opts, :order_by, :id)

    cursor =
      Map.get_lazy(opts, :cursor, fn ->
        # Not robust: fix later
        (schema |> select([el], min(field(el, ^order_by))) |> repo.one!) - 1
      end)

    %{batch_size: batch_size, mode: :cursor, order_by: order_by, cursor: cursor}
  end

  defp create_stream(repo, schema, initial_params) do
    Stream.unfold(initial_params, fn params ->
      results = schema |> build_query(params) |> repo.all

      case results do
        [] ->
          nil

        results ->
          next_params = build_next_params(results, params)
          {results, next_params}
      end
    end)
  end

  defp build_query(schema, %{
         mode: :offset,
         batch_size: batch_size,
         order_by: order_by,
         offset: offset
       }) do
    schema |> limit(^batch_size) |> offset(^offset) |> order_by(^order_by)
  end

  defp build_query(schema, %{
         mode: :cursor,
         batch_size: batch_size,
         order_by: order_by,
         cursor: cursor
       }) do
    schema
    |> limit(^batch_size)
    |> where([el], field(el, ^order_by) > ^cursor)
    |> order_by(^order_by)
  end

  defp build_next_params(_results, params = %{mode: :offset}) do
    Map.update!(params, :offset, &(&1 + params.batch_size))
  end

  defp build_next_params(results, params = %{mode: :cursor, order_by: order_by}) do
    next_cursor = next_cursor_fun(results, order_by)
    %{params | cursor: next_cursor}
  end

  defp next_cursor_fun(results, order_by) do
    results |> List.last() |> Map.fetch!(order_by)
  end
end
