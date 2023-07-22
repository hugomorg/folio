defmodule FolioTest do
  use ExUnit.Case
  doctest Folio

  test "greets the world" do
    assert Folio.hello() == :world
  end
end
