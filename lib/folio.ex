defmodule Folio do
  @moduledoc """
  Documentation for `Folio`.
  """

  @doc """
  TBC
  """

  import Ecto.Query

  def page(repo, schema, opts \\ []) do
    mode = Keyword.get(opts, :mode, :cursor)

    create_stream(repo, schema, build_opts(opts, mode))
  end

  defp build_opts(opts, :offset) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    offset = Keyword.get(opts, :offset, 0)
    order_by = Keyword.get(opts, :order_by, :id)
    %{batch_size: batch_size, offset: offset, mode: :offset, order_by: order_by}
  end

  defp create_stream(repo, schema, initial_params) do
    Stream.unfold(initial_params, fn params ->
      results = schema |> build_query(params) |> repo.all

      case results do
        [] ->
          nil

        results ->
          next_params = Map.update!(params, :offset, &(&1 + params.batch_size))
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
end
