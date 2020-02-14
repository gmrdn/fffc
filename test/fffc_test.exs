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

  
end
