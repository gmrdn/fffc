defmodule Fffc do
  def read_csv_file(filename) do
    File.read!(filename)
    |> String.split("\n")
  end

  def parse_meta_data(meta) do
    Enum.map_reduce(meta, 0, fn x, acc ->
      line = String.split(x, ",")

      {[
         name: Enum.at(line, 0),
         length: String.to_integer(Enum.at(line, 1)),
         type: Enum.at(line, 2),
         range: acc..(acc + String.to_integer(Enum.at(line, 1)) - 1)
       ], acc + String.to_integer(Enum.at(line, 1))}
    end)
  end

  def get_csv_headers(columns) do
    elem(columns, 0)
    |> Enum.map(fn x ->
      Keyword.get(x, :name)
    end)
    |> Enum.join(",")
  end

  def convert_raw_line_to_csv(columns, line) do
    elem(columns, 0)
    |> Enum.map(fn x ->
      String.slice(line, Keyword.get(x, :range))
      |> format_value(Keyword.get(x, :type))
    end)
    |> Enum.join(",")
  end

  def format_value(value, "string") do
    if String.contains?(value, ",") do
      String.trim("\"" <> String.replace(value, "\"", "\"\"") <> "\"")
    else
      String.trim(String.replace(value, "\"", "\"\""))
    end
  end

  def format_value(value, "date") do
    date = Date.from_iso8601!(value)

    [date.day, date.month, date.year]
    |> Enum.map(fn x -> to_string(x) end)
    |> Enum.map(fn x -> String.pad_leading(x, 2, "0") end)
    |> Enum.join("/")
  end

  def format_value(value, "numeric") do
    elem(Float.parse(String.trim(value)), 0)
  end

  def prepare_stream_pipeline(columns, filename) do
    File.stream!(filename)
    |> Stream.map(fn x ->
      [convert_raw_line_to_csv(columns, x), "\n"]
    end)
  end

  def write_stream_to_csv(headers, stream, filename) do
    File.mkdir('output')
    File.write!(filename, headers <> "\n")

    stream
    |> Stream.into(File.stream!(filename, [:append, :utf8]))
    |> Stream.run()
  end
end
