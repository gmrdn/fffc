defmodule FffcTest do
  use ExUnit.Case
  doctest Fffc

  describe "Meta data parsing" do
    test "Should convert a line of metadata (csv format) to a list of keywords and values" do
      meta = ["Birth date,10,date"]
      columns = elem(Fffc.parse_meta_data(meta), 0)
      assert Enum.at(columns, 0) |> Keyword.get(:name) == "Birth date"
      assert Enum.at(columns, 0) |> Keyword.get(:length) == 10
      assert Enum.at(columns, 0) |> Keyword.get(:type) == "date"
    end

    test "Should convert all lines of metada (csv format) to a list of columns with keywords and values" do
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

    test "Should add all columns length and returns the total length" do
      meta = [
        "Col1,1,string",
        "Col2,2,string"
      ]

      columns_length = elem(Fffc.parse_meta_data(meta), 1)
      assert columns_length == 3
    end

    test "Should add the begining and ending position (range) of the column in the line" do
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

    test "Should read meta data from csv file" do
      meta = Fffc.read_csv_file('test_files/example_meta.csv')

      assert meta == [
               "Birth date,10,date",
               "First name,15,string",
               "Last name,15,string",
               "Weight,5,numeric"
             ]
    end

    test "Should format columns names as csv headers" do
      columns =
        Fffc.parse_meta_data([
          "Birth date,10,date",
          "First name,15,string",
          "Last name,15,string",
          "Weight,5,numeric"
        ])

      assert Fffc.get_csv_headers(columns) == "Birth date,First name,Last name,Weight"
    end
  end

  describe "Data parsing with one line" do
    test "Should parse data with one string column" do
      columns = Fffc.parse_meta_data(["First name,15,string"])
      data_row = "Guillaume      "
      assert Fffc.convert_raw_line_to_csv(columns, data_row) == "Guillaume"
    end

    test "Should parse data with two string columns" do
      columns = Fffc.parse_meta_data(["First name,15,string", "Last name,15,string"])
      data_row = "Guillaume      Rondon         "
      assert Fffc.convert_raw_line_to_csv(columns, data_row) == "Guillaume,Rondon"
    end

    test "Should parse data with Date, string and numeric, all valid" do
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

    test "Should parse data with special characters" do
      columns = Fffc.parse_meta_data(["First name,31,string"])
      data_row = " !#$%&'()*+-./:;<=>?@[\\]^_`{|}~"

      assert Fffc.convert_raw_line_to_csv(columns, data_row) ==
               "!#$%&'()*+-./:;<=>?@[\\]^_`{|}~"
    end

    test "Should add double quotes when a value contains a coma" do
      columns = Fffc.parse_meta_data(["First name,7,string"])
      data_row = "Aaa,bbb"

      assert Fffc.convert_raw_line_to_csv(columns, data_row) ==
               "\"Aaa,bbb\""
    end

    test "Should add double quotes when a value contains a coma and double all double quotes inside the string" do
      columns = Fffc.parse_meta_data(["First name,7,string"])
      data_row = "A\"a,b\"b"

      assert Fffc.convert_raw_line_to_csv(columns, data_row) ==
               "\"A\"\"a,b\"\"b\""
    end

    test "Should exit the program if a line of data has an incorrect length" do
      columns = Fffc.parse_meta_data(["First name,15,string", "Last name,15,string"])
      data_row = "Guillaume                   Rondon                     "

      assert_raise ArgumentError,
                   "incorrect line's length: expected 30 received : 55",
                   fn ->
                     Fffc.convert_raw_line_to_csv(columns, data_row)
                   end
    end
  end

  describe "Data parsing with a stream of lines" do
    test "Should prepare a small stream of three lines" do
      columns =
        Fffc.parse_meta_data([
          "Birth date,10,date",
          "First name,15,string",
          "Last name,15,string",
          "Weight,5,numeric"
        ])

      stream = Fffc.prepare_stream_pipeline(columns, 'test_files/example_data.txt')

      assert Enum.take(stream, 3) == [
               ["01/01/1970,John,Smith,81.5", "\n"],
               ["31/01/1975,Jane,Doe,61.1", "\n"],
               ["28/11/1988,Bob,Big,102.4", "\n"]
             ]
    end

    test "Should write stream to csv, small file" do
      columns = Fffc.parse_meta_data(Fffc.read_csv_file('test_files/example_meta.csv'))
      headers = Fffc.get_csv_headers(columns)
      stream = Fffc.prepare_stream_pipeline(columns, 'test_files/example_data.txt')
      result = Fffc.write_stream_to_csv(headers, stream, 'output/test_example.csv')
      assert result == :ok
    end

    test "Should include column names first" do
      columns = Fffc.parse_meta_data(Fffc.read_csv_file('test_files/example_meta.csv'))
      headers = Fffc.get_csv_headers(columns)
      stream = Fffc.prepare_stream_pipeline(columns, 'test_files/example_data.txt')
      Fffc.write_stream_to_csv(headers, stream, 'output/test_example.csv')

      assert File.read!('output/test_example.csv') |> String.split("\n") |> List.first() ==
               "Birth date,First name,Last name,Weight"
    end

    test "Should write stream to csv, special characters" do
      columns = Fffc.parse_meta_data(Fffc.read_csv_file('test_files/special_chars_meta.csv'))
      headers = Fffc.get_csv_headers(columns)
      stream = Fffc.prepare_stream_pipeline(columns, 'test_files/special_chars_data.txt')
      result = Fffc.write_stream_to_csv(headers, stream, 'output/test_special_chars.csv')
      assert result == :ok
    end
  end

  describe "Date formatting" do
    test "Should format the date as dd/mm/yyyy" do
      assert Fffc.format_value("1988-11-28", "date") == "28/11/1988"
    end

    test "Should format the date as dd/mm/yyyy, adding zeros when necessary" do
      assert Fffc.format_value("1970-01-01", "date") == "01/01/1970"
    end

    test "Should raise an error when the date format is incorrect" do
      assert_raise ArgumentError, "cannot parse \"abc\" as date, reason: :invalid_format", fn ->
        Fffc.format_value("abc", "date")
      end
    end

    test "Should raise an error when the date is empty" do
      assert_raise ArgumentError, "cannot parse \"\" as date, reason: :invalid_format", fn ->
        Fffc.format_value("", "date")
      end
    end
  end
end
