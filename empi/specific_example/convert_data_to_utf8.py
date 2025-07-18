
# Set input and output file paths
input_file = "TuvaPatientImportFile.txt"
output_file = "TuvaPatientImportFile_UTF8.txt"


# Read using a more tolerant enconding like latin1 or windows-1252
with open(input_file, "r", encoding="latin1") as infile:
    contents = infile.read()

# Write using UTF-8 encoding:
with open(output_file, "w", encoding="utf-8") as outfile:
    outfile.write(contents)

print(f"File successfully converted to UTF-8 and saved as {output_file}")

