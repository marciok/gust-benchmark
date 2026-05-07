defmodule BenchmarkMockIo do
  use Gust.DSL

  require Logger
  alias Gust.Flows

  @tmp_dir System.get_env("BENCHMARK_TMP_DIR", "/tmp/gust_benchmark")
  @mock_server_url System.get_env("BENCHMARK_MOCK_SERVER_URL", "http://mock-server:8000")

  defp ensure_tmp_dir! do
    File.mkdir_p!(@tmp_dir)
  end

  defp run_dir(run_id), do: Path.join(@tmp_dir, "run_#{run_id}")
  defp raw_path(run_id), do: Path.join(run_dir(run_id), "raw.json")
  defp transformed_path(run_id), do: Path.join(run_dir(run_id), "transformed.json")

  defp duration_ms(started_at) do
    System.monotonic_time(:millisecond) - started_at
  end

  task :extract_data, downstream: [:transform_data], save: true, ctx: %{run_id: run_id} do
    started_at = System.monotonic_time(:millisecond)

    ensure_tmp_dir!()
    File.mkdir_p!(run_dir(run_id))

    Logger.info("[extract_data] calling #{@mock_server_url}/extract")

    response = Req.get!(
      "#{@mock_server_url}/extract", 
        connect_options: [timeout: 120_000],
        receive_timeout: 120_000
    )
    payload = response.body

    path = raw_path(run_id)
    File.write!(path, Jason.encode!(payload))

    count = Map.get(payload, "count", 0)

    Logger.info(
      "[extract_data] done duration_ms=#{duration_ms(started_at)} count=#{count} path=#{path}"
    )

    %{
      raw_path: path,
      count: count,
      duration_ms: duration_ms(started_at)
    }
  end

  task :transform_data, downstream: [:load_data], save: true, ctx: %{run_id: run_id} do
    started_at = System.monotonic_time(:millisecond)

    extract_task = Flows.get_task_by_name_run("extract_data", run_id)
    input_path = extract_task.result["raw_path"]
    output_path = transformed_path(run_id)

    Logger.info("[transform_data] reading #{input_path}")

    payload =
      input_path
      |> File.read!()
      |> Jason.decode!()

    items = Map.get(payload, "items", [])

    transformed_items =
      Enum.map(items, fn item ->
        price = item["price"] * 1.0
        year = item["year"]
        mileage = item["mileage"]

        price_bucket =
          cond do
            price < 20_000 -> "cheap"
            price < 50_000 -> "mid"
            true -> "premium"
          end

        normalized_score = Float.round(year * 1000 / (mileage + 1), 6)

        %{
          source_id: item["id"],
          brand: item["brand"] |> String.trim() |> String.downcase(),
          model: item["model"] |> String.trim() |> String.downcase(),
          year: year,
          price: price,
          mileage: mileage,
          price_bucket: price_bucket,
          normalized_score: normalized_score
        }
      end)

    transformed_payload = %{
      items: transformed_items,
      count: length(transformed_items)
    }

    File.write!(output_path, Jason.encode!(transformed_payload))

    Logger.info(
      "[transform_data] done duration_ms=#{duration_ms(started_at)} count=#{length(transformed_items)} path=#{output_path}"
    )

    %{
      transformed_path: output_path,
      count: length(transformed_items),
      duration_ms: duration_ms(started_at)
    }
  end

  task :load_data, save: true, ctx: %{run_id: run_id} do
    started_at = System.monotonic_time(:millisecond)

    transform_task = Flows.get_task_by_name_run("transform_data", run_id)
    path = transform_task.result["transformed_path"]

    Logger.info("[load_data] posting #{path} to #{@mock_server_url}/load")

    payload =
      path
      |> File.read!()
      |> Jason.decode!()

    response =
      Req.post!(
        "#{@mock_server_url}/load",
        connect_options: [timeout: 120_000],
        receive_timeout: 120_000,
        json: payload
      )

    result = response.body

    Logger.info(
      "[load_data] done duration_ms=#{duration_ms(started_at)} received=#{result["received"]} ok=#{result["ok"]}"
    )

    %{
      ok: result["ok"],
      received: result["received"],
      duration_ms: duration_ms(started_at)
    }
  end
end
