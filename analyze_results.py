#!/usr/bin/env python3
"""
Script to analyze container network information results.
Finds containers listening on specific ports and outputs results.
"""

import json
import argparse
import csv
import sys
from collections import defaultdict
from typing import Dict, List, Tuple


def parse_jsonl_file(filename: str) -> List[dict]:
    """Parse JSONL file (one JSON object per line or multiple JSON objects)."""
    containers = []
    
    with open(filename, 'r') as f:
        buffer = ""
        brace_count = 0
        for line in f:
            buffer += line
            brace_count += line.count('{') - line.count('}')
            
            if brace_count == 0 and buffer.strip():
                # Complete JSON object
                try:
                    container = json.loads(buffer.strip())
                    containers.append(container)
                except json.JSONDecodeError:
                    pass
                buffer = ""
    
    return containers


def find_containers_on_port(containers: List[dict], port: int) -> List[dict]:
    """Find all containers that have the specified port open."""
    matching_containers = []
    
    for container in containers:
        open_ports = container.get('open_ports', [])
        for port_info in open_ports:
            if port_info.get('port') == port:
                matching_containers.append(container)
                break
    
    return matching_containers


def group_by_image(containers: List[dict]) -> Dict[str, List[str]]:
    """Group container IDs by image name."""
    images = defaultdict(list)
    
    for container in containers:
        image = container.get('image', 'unknown')
        container_id = container.get('id', 'unknown')
        images[image].append(container_id)
    
    return dict(images)


def print_summary(images: Dict[str, List[str]], port: int):
    """Print a summary of images listening on the specified port."""
    total_containers = sum(len(ids) for ids in images.values())
    
    print(f"\n{'='*80}")
    print(f"Containers listening on port {port}")
    print(f"{'='*80}")
    print(f"\nTotal: {total_containers} containers across {len(images)} unique image(s)\n")
    
    # Sort by count (descending)
    sorted_images = sorted(images.items(), key=lambda x: len(x[1]), reverse=True)
    
    for image, container_ids in sorted_images:
        print(f"Image: {image}")
        print(f"  Count: {len(container_ids)} container(s)")
        if len(container_ids) <= 5:
            print(f"  Container IDs:")
            for cid in container_ids:
                print(f"    - {cid}")
        else:
            print(f"  Container IDs (showing first 5 of {len(container_ids)}):")
            for cid in container_ids[:5]:
                print(f"    - {cid}")
        print()


def write_csv(images: Dict[str, List[str]], port: int, output_file: str):
    """Write results to CSV file."""
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['Image', 'Container Count', 'Container IDs'])
        
        # Sort by count (descending)
        sorted_images = sorted(images.items(), key=lambda x: len(x[1]), reverse=True)
        
        for image, container_ids in sorted_images:
            # Join container IDs with semicolon for CSV
            container_ids_str = ';'.join(container_ids)
            writer.writerow([image, len(container_ids), container_ids_str])
    
    print(f"Results written to {output_file}")


def write_detailed_csv(containers: List[dict], port: int, output_file: str):
    """Write detailed CSV with one row per container."""
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        # Write header
        writer.writerow(['Container ID', 'Image', 'Port', 'Type'])
        
        for container in containers:
            container_id = container.get('id', 'unknown')
            image = container.get('image', 'unknown')
            open_ports = container.get('open_ports', [])
            
            # Find the port 443 entry
            for port_info in open_ports:
                if port_info.get('port') == port:
                    port_type = port_info.get('type', 'unknown')
                    writer.writerow([container_id, image, port, port_type])
                    break
    
    print(f"Detailed results written to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description='Analyze container network information results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Find containers on port 443 and print summary
  %(prog)s results.json --port 443

  # Output to CSV (summary format)
  %(prog)s results.json --port 443 --csv output.csv

  # Output detailed CSV (one row per container)
  %(prog)s results.json --port 443 --csv output.csv --detailed
        """
    )
    
    parser.add_argument(
        'input_file',
        help='Input JSON file with container network information'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=443,
        help='Port number to search for (default: 443)'
    )
    
    parser.add_argument(
        '--csv',
        metavar='OUTPUT_FILE',
        help='Output results to CSV file'
    )
    
    parser.add_argument(
        '--detailed',
        action='store_true',
        help='Output detailed CSV with one row per container (only with --csv)'
    )
    
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Suppress summary output (useful when writing to CSV)'
    )
    
    args = parser.parse_args()
    
    # Parse the input file
    try:
        print(f"Parsing {args.input_file}...")
        containers = parse_jsonl_file(args.input_file)
        print(f"Found {len(containers)} containers")
    except FileNotFoundError:
        print(f"Error: File '{args.input_file}' not found", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Find containers on the specified port
    matching_containers = find_containers_on_port(containers, args.port)
    print(f"Found {len(matching_containers)} containers listening on port {args.port}")
    
    if not matching_containers:
        print("No containers found listening on the specified port.")
        sys.exit(0)
    
    # Group by image
    images = group_by_image(matching_containers)
    
    # Print summary unless quiet mode
    if not args.quiet:
        print_summary(images, args.port)
    
    # Write CSV if requested
    if args.csv:
        if args.detailed:
            write_detailed_csv(matching_containers, args.port, args.csv)
        else:
            write_csv(images, args.port, args.csv)


if __name__ == '__main__':
    main()

