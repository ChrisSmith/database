#!/usr/bin/env python3
"""
TPC-H Benchmark Results Visualization Script

This script reads benchmark_results.json and creates grouped bar charts
showing query performance across different database systems, sorted by
average execution time from best to worst.
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse
from typing import Dict, List, Tuple, Optional

def load_benchmark_results(file_path: str) -> Dict:
    """Load benchmark results from JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def calculate_system_averages(results: Dict) -> List[Tuple[str, float]]:
    """
    Calculate average execution time for each system, excluding failed/timed out queries.
    Returns list of (system_name, avg_time) tuples sorted by avg_time.
    """
    system_averages = []
    
    for system_name, queries in results['RunnerResults'].items():
        total_time = 0
        successful_queries = 0
        
        for query_id, result in queries.items():
            if result['Status'] == 'OK' and result['ElapsedMs'] > 0:
                total_time += result['ElapsedMs']
                successful_queries += 1
        
        if successful_queries > 0:
            avg_time = total_time / successful_queries
            system_averages.append((system_name, avg_time))
    
    # Sort by average time (ascending - best to worst)
    return sorted(system_averages, key=lambda x: x[1])

def prepare_plot_data(results: Dict, sorted_systems: List[str]) -> Tuple[List[str], List[float], List[bool]]:
    """
    Prepare data for plotting grouped by system.
    Returns x-axis labels, execution times, and failed query flags.
    """
    # Get all query IDs (assuming they're consistent across systems)
    first_system = list(results['RunnerResults'].values())[0]
    query_ids = sorted([int(qid) for qid in first_system.keys()])
    
    # Create lists for plotting data
    x_labels = []
    times = []
    failed_flags = []
    
    # Group by system: all queries for system 1, then all queries for system 2, etc.
    for system in sorted_systems:
        system_data = results['RunnerResults'][system]
        for query_id in query_ids:
            query_str = str(query_id)
            x_labels.append(f"{query_id+1:02d}")  # Format as 01, 02, 03, etc.
            
            if query_str in system_data:
                result = system_data[query_str]
                if result['Status'] == 'OK' and result['ElapsedMs'] > 0:
                    times.append(result['ElapsedMs'])
                    failed_flags.append(False)
                else:
                    times.append(0)  # Will be shown as failed
                    failed_flags.append(True)
            else:
                times.append(0)
                failed_flags.append(True)
    
    return x_labels, times, failed_flags

def create_grouped_bar_chart(x_labels: List[str], times: List[float], failed_flags: List[bool],
                           system_names: List[str], output_file: Optional[str] = None):
    """Create and display/save grouped bar chart."""
    
    # Set up the plot
    fig, ax = plt.subplots(figsize=(20, 10))
    
    n_total_bars = len(x_labels)
    n_systems = len(system_names)
    n_queries_per_system = n_total_bars // n_systems
    
    # Create positions for bars
    x_pos = np.arange(n_total_bars)
    
    # Create color mapping for systems
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2']
    bar_colors = []
    for i in range(n_systems):
        color = colors[i % len(colors)]
        bar_colors.extend([color] * n_queries_per_system)
    
    # Plot bars
    bars = ax.bar(x_pos, times, color=bar_colors, alpha=0.8, width=0.8)
    
    # Add hatching for failed queries
    for bar, is_failed in zip(bars, failed_flags):
        if is_failed:
            bar.set_hatch('///')
            bar.set_alpha(0.3)
    
    # Customize the plot
    ax.set_xlabel('Database Systems and Queries', fontsize=12, fontweight='bold')
    ax.set_ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
    ax.set_title('TPC-H Benchmark Results - Query Execution Times Grouped by Database System\n' +
                'Systems ordered by average execution time (best to worst)', 
                fontsize=14, fontweight='bold', pad=20)
    
    # Set x-axis
    ax.set_xticks(x_pos)
    ax.set_xticklabels(x_labels, rotation=45, ha='right', fontsize=8)
    
    # Add vertical lines to separate systems
    for i in range(1, n_systems):
        separator_pos = i * n_queries_per_system - 0.5
        ax.axvline(x=separator_pos, color='black', linestyle='--', alpha=0.5, linewidth=1)
    
    # Create custom legend
    legend_elements = []
    for i, system in enumerate(system_names):
        color = colors[i % len(colors)]
        legend_elements.append(plt.Rectangle((0,0),1,1, facecolor=color, alpha=0.8, label=system))
    
    legend = ax.legend(handles=legend_elements, bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
    legend.set_title('Database Systems\n(ordered by avg performance)', prop={'weight': 'bold'})
    
    # Add grid for better readability
    ax.grid(True, alpha=0.3, axis='y')
    
    # Add system labels at the top
    for i, system in enumerate(system_names):
        center_pos = i * n_queries_per_system + (n_queries_per_system - 1) / 2
        ax.text(center_pos, ax.get_ylim()[1] * 1.1, system, 
               ha='center', va='bottom', fontweight='bold', fontsize=11,
               bbox=dict(boxstyle='round,pad=0.3', facecolor=colors[i % len(colors)], alpha=0.3))
    
    # Add note about failed queries
    ax.text(0.02, 0.98, 'Note: Hatched bars indicate failed/timed out queries\nVertical dashed lines separate database systems', 
            transform=ax.transAxes, fontsize=9, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    # Adjust layout to prevent clipping
    plt.tight_layout()
    
    # Save or show
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Chart saved to: {output_file}")
    else:
        plt.show()

def print_summary_statistics(results: Dict, sorted_systems: List[Tuple[str, float]]):
    """Print summary statistics for each system."""
    print("\n" + "="*80)
    print("BENCHMARK SUMMARY STATISTICS")
    print("="*80)
    
    for rank, (system_name, avg_time) in enumerate(sorted_systems, 1):
        system_data = results['RunnerResults'][system_name]
        
        total_queries = len(system_data)
        successful = sum(1 for r in system_data.values() if r['Status'] == 'OK' and r['ElapsedMs'] > 0)
        failed = sum(1 for r in system_data.values() if r['Status'] == 'FAILED')
        timed_out = sum(1 for r in system_data.values() if r['Status'] == 'TIMED OUT')
        
        # Calculate min/max for successful queries
        successful_times = [r['ElapsedMs'] for r in system_data.values() 
                          if r['Status'] == 'OK' and r['ElapsedMs'] > 0]
        min_time = min(successful_times) if successful_times else 0
        max_time = max(successful_times) if successful_times else 0
        
        print(f"\n#{rank}. {system_name}")
        print(f"    Average Time: {avg_time:.1f} ms")
        print(f"    Min Time: {min_time} ms")
        print(f"    Max Time: {max_time} ms")
        print(f"    Success Rate: {successful}/{total_queries} ({successful/total_queries*100:.1f}%)")
        if failed > 0:
            print(f"    Failed: {failed}")
        if timed_out > 0:
            print(f"    Timed Out: {timed_out}")

def main():
    parser = argparse.ArgumentParser(description='Visualize TPC-H benchmark results')
    parser.add_argument('--input', '-i', 
                       default='src/Database.BenchmarkRunner/benchmark_results.json',
                       help='Path to benchmark_results.json file')
    parser.add_argument('--output', '-o', 
                       help='Output file path for the chart (optional)')
    parser.add_argument('--no-show', action='store_true',
                       help='Don\'t display the chart (only save to file)')
    
    args = parser.parse_args()
    
    # Check if input file exists
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: Input file '{args.input}' not found!")
        return 1
    
    try:
        # Load and process data
        print(f"Loading benchmark results from: {args.input}")
        results = load_benchmark_results(args.input)
        
        # Calculate averages and sort systems
        sorted_systems = calculate_system_averages(results)
        system_names = [name for name, _ in sorted_systems]
        
        print(f"Found {len(system_names)} database systems with results")
        
        # Print summary statistics
        print_summary_statistics(results, sorted_systems)
        
        # Prepare plot data
        x_labels, times, failed_flags = prepare_plot_data(results, system_names)
        
        # Create visualization
        n_queries = len(x_labels) // len(system_names)
        print(f"\nCreating visualization with {n_queries} queries per system...")
        if not args.no_show or args.output:
            create_grouped_bar_chart(x_labels, times, failed_flags, system_names, 
                                    args.output if args.output else None)
        
        if not args.output and not args.no_show:
            input("Press Enter to close...")
            
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
