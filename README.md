# README

Single file test:

```bash
echo s3://s3-frankfurt-1010101/import_template.csv | dotnet run > xx 2>deb
```

Multiple files test:

```bash
cat input_list.txt | dotnet run > yy 2>deb
```