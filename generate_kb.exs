# Create a ~500MB KB
fact_count = 5_000_000
file_path = "kb_large.pl"

IO.puts "Generating #{fact_count} facts in #{file_path}..."

File.open!(file_path, [:write, :delayed_write], fn file ->
  # Add some logic
  IO.write(file, "is_valid(ID) :- fact(ID, _).\n")
  
  Enum.each(1..fact_count, fn i ->
    if rem(i, 1_000_000) == 0, do: IO.puts "Wrote #{i} facts..."
    IO.write(file, "fact(#{i}, #{rem(i, 100)}).\n")
  end)
end)

IO.puts "Done."
