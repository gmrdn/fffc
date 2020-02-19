defmodule Commandline.CLI do
  def main(args) do
    options = [
      switches: [meta: :string, data: :string, output: :string],
      aliases: [m: :meta, d: :data, o: :output]
    ]

    {opts, _, _} = OptionParser.parse(args, options)
    IO.inspect(opts, label: "Command Line Arguments")
    IO.puts("Parsing meta data... ")
    columns = Fffc.parse_meta_data(Fffc.read_csv_file(opts[:meta]))
    IO.puts("Retrieving columns names...")
    headers = Fffc.get_csv_headers(columns)
    IO.puts("Preparing to stream data file...")
    stream = Fffc.prepare_stream_pipeline(columns, opts[:data])
    IO.puts("Processing data file stream...")
    result = Fffc.write_stream_to_csv(headers, stream, opts[:output])

    case result do
      :ok ->
        IO.puts("Success")

      _ ->
        IO.puts("Failed to parse data file")
    end
  end
end
