# Container Network Information Tool

Script to iterate over all containers and find open ports from Prisma Cloud API.

## Main Script

### Usage
```bash
go run main.go
```

### Environment Variables Required
- `tlUrl` - Prisma Cloud console URL (e.g., `https://api.prismacloud.io`)
- `pcIdentity` - Prisma Cloud access key
- `pcSecret` - Prisma Cloud secret key

### Options
- `--debug` - Enable debug logging to see detailed information about the processing

### Output
The script outputs JSON to stdout with the following format:
```json
{
  "id": "container_id",
  "image": "image_name:tag",
  "open_ports": [
    {
      "port": 443,
      "type": "firewallProtection_tls"
    }
  ]
}
```

To save output to a file:
```bash
go run main.go > results.json
```

## Post-Processing Script

The `analyze_results.py` script analyzes the results and provides summary statistics with optional CSV export.

### Usage

#### Print summary to console
```bash
python3 analyze_results.py results.json --port 443
```

#### Output summary to CSV (grouped by image)
```bash
python3 analyze_results.py results.json --port 443 --csv output.csv
```

#### Output detailed CSV (one row per container)
```bash
python3 analyze_results.py results.json --port 443 --csv output.csv --detailed
```

#### Search for a different port
```bash
python3 analyze_results.py results.json --port 8080 --csv port_8080.csv
```

#### Quiet mode (only CSV output, no console summary)
```bash
python3 analyze_results.py results.json --port 443 --csv output.csv --quiet
```

### Options
- `input_file` - Input JSON file with container network information (required)
- `--port PORT` - Port number to search for (default: 443)
- `--csv OUTPUT_FILE` - Output results to CSV file
- `--detailed` - Output detailed CSV with one row per container (only with --csv)
- `--quiet` - Suppress summary output (useful when writing to CSV)

### CSV Output Formats

**Summary Format** (default with `--csv`):
- Columns: `Image`, `Container Count`, `Container IDs`
- One row per unique image
- Container IDs are semicolon-separated

**Detailed Format** (with `--detailed`):
- Columns: `Container ID`, `Image`, `Port`, `Type`
- One row per container

### Example Output

When analyzing port 443:
```
Found 225 containers listening on port 443

================================================================================
Containers listening on port 443
================================================================================

Total: 225 containers across 24 unique image(s)

Image: abc:123
  Count: 89 container(s)
  ...
```


