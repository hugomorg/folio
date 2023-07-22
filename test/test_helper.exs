Application.ensure_all_started(:postgrex)
{:ok, _} = Folio.TestRepo.start_link()
ExUnit.start()
