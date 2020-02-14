defmodule Fffc do
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
end
