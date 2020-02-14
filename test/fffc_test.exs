defmodule FffcTest do
  use ExUnit.Case
  doctest Fffc

  describe "Meta data parsing" do
    test "Converts a line of metadata (csv format) to a list of keywords and values" do
      meta = ["Birth date,10,date"]
      columns = elem(Fffc.parse_meta_data(meta), 0)
      assert Enum.at(columns, 0) |> Keyword.get(:name) == "Birth date"
      assert Enum.at(columns, 0) |> Keyword.get(:length) == 10
      assert Enum.at(columns, 0) |> Keyword.get(:type) == "date"
    end

    test "Converts all lines of metada (csv format) to a list of columns with keywords and values" do
      meta = [
        "Birth date,10,date",
        "First name,15,string",
        "Last name,15,string",
        "Weight,5,numeric"
      ]

      columns = elem(Fffc.parse_meta_data(meta), 0)
      assert Enum.at(columns, 0) |> Keyword.get(:name) == "Birth date"
      assert Enum.at(columns, 1) |> Keyword.get(:name) == "First name"
      assert Enum.at(columns, 3) |> Keyword.get(:type) == "numeric"
    end

    test "Adds all columns length and returns the total length" do
      meta = [
        "Col1,1,string",
        "Col2,2,string"
      ]

      columns_length = elem(Fffc.parse_meta_data(meta), 1)
      assert columns_length == 3
    end

    test "Adds the begining and ending position (range) of the column in the line" do
      meta = [
        "Birth date,10,date",
        "First name,15,string",
        "Last name,15,string",
        "Weight,5,numeric"
      ]

      columns = elem(Fffc.parse_meta_data(meta), 0)
      assert Enum.at(columns, 0) |> Keyword.get(:range) == 0..9
      assert Enum.at(columns, 1) |> Keyword.get(:range) == 10..24
      assert Enum.at(columns, 2) |> Keyword.get(:range) == 25..39
      assert Enum.at(columns, 3) |> Keyword.get(:range) == 40..44
    end

    test "Can read meta data from csv file" do
      meta = Fffc.read_csv_file('test_files/example_meta.csv')

      assert meta == [
               "Birth date,10,date",
               "First name,15,string",
               "Last name,15,string",
               "Weight,5,numeric"
             ]
    end
  end

  describe "Data parsing with one line" do
    test "One string column" do
      columns = Fffc.parse_meta_data(["First name,15,string"])
      data_row = "Guillaume      "
      assert Fffc.convert_raw_line_to_csv(columns, data_row) == "Guillaume"
    end

    test "Two string columns" do
      columns = Fffc.parse_meta_data(["First name,15,string", "Last name,15,string"])
      data_row = "Guillaume      Rondon         "
      assert Fffc.convert_raw_line_to_csv(columns, data_row) == "Guillaume,Rondon"
    end

    test "Date, string and numeric, all valid" do
      columns =
        Fffc.parse_meta_data([
          "Birth date,10,date",
          "First name,15,string",
          "Last name,15,string",
          "Weight,5,numeric"
        ])

      data_row = "1988-11-28Bob            Big            102.4"
      assert Fffc.convert_raw_line_to_csv(columns, data_row) == "28/11/1988,Bob,Big,102.4"
    end
  end

  describe "Data parsing with a stream of lines" do
    test "Small stream of three lines" do
      columns =
        Fffc.parse_meta_data([
          "Birth date,10,date",
          "First name,15,string",
          "Last name,15,string",
          "Weight,5,numeric"
        ])

      stream = Fffc.prepare_stream_pipeline(columns, 'test_files/example_data.txt')

      assert Enum.take(stream, 3) == [
               "01/01/1970,John,Smith,81.5",
               "31/01/1975,Jane,Doe,61.1",
               "28/11/1988,Bob,Big,102.4"
             ]
    end

    test "Write stream to csv, small files" do
      stream =
        Fffc.read_csv_file('test_files/example_meta.csv')
        |> Fffc.parse_meta_data()
        |> Fffc.prepare_stream_pipeline('test_files/example_data.txt')

      res = Fffc.write_stream_to_csv(stream, 'output/test_example.csv')
      assert res == :ok
    end
  end

  describe "Date formatting" do
    test "Format the date as dd/mm/yyyy" do
      assert Fffc.format_value("1988-11-28", "date") == "28/11/1988"
    end

    test "Format the date as dd/mm/yyyy, adding zeros when necessary" do
      assert Fffc.format_value("1970-01-01", "date") == "01/01/1970"
    end
  end
end
