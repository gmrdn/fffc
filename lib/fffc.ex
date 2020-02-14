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

  def convert_raw_line_to_csv(columns, line) do
    elem(columns, 0)
    |> Enum.map(fn x ->
      String.slice(line, Keyword.get(x, :range))
      |> format_value(Keyword.get(x, :type))
    end)
    |> Enum.join(",")
  end

  def format_value(value, "string") do
    String.trim(value)
  end

  def format_value(value, "date") do
    date = elem(Date.from_iso8601(value), 1)

    [date.day, date.month, date.year]
    |> Enum.map(fn x -> to_string(x) end)
    |> Enum.map(fn x -> String.pad_leading(x, 2, "0") end)
    |> Enum.join("/")
  end

  def format_value(value, "numeric") do
    value
  end
end
