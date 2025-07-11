import csv
import matplotlib.pyplot as plt
from collections import defaultdict

performance_csv_path = "load_mode_comparison.csv"

def gather_performance_stats(csv_location):
    blueprint = defaultdict(lambda: {'record_batch': [], 'run_seconds': [], 'speed_rate': [], 'lag_delay': []})
    with open(csv_location, 'r') as chart_file:
        row_scanner = csv.DictReader(chart_file)
        for timeline in row_scanner:
            method_type = timeline['Mode']
            blueprint[method_type]['record_batch'].append(int(timeline['Load']))
            blueprint[method_type]['run_seconds'].append(float(timeline['TimeSec']))
            blueprint[method_type]['speed_rate'].append(float(timeline['Throughput']))
            blueprint[method_type]['lag_delay'].append(float(timeline['Latency']))
    return blueprint

def sketch_bars(input_tracker, height_data_map, y_axis_label, plot_caption, filename_for_save):
    plt.figure(figsize=(10, 6))
    bar_width = 0.25
    stream_modes = list(height_data_map.keys())
    all_loads = sorted(set(each for bundle in input_tracker.values() for each in bundle))
    load_index = {val: idx for idx, val in enumerate(all_loads)}
    shifts = [-bar_width, 0, bar_width]

    for place, stream in enumerate(stream_modes):
        offset_locs = [load_index[item] + shifts[place] for item in input_tracker[stream]]
        plt.bar(
            offset_locs,
            height_data_map[stream],
            width=bar_width,
            label=stream
        )

    plt.title(plot_caption)
    plt.xlabel("Input Size")
    plt.ylabel(y_axis_label)
    plt.xticks(range(len(all_loads)), all_loads)
    plt.legend()
    plt.grid(axis='y')
    plt.tight_layout()
    plt.savefig(filename_for_save)
    plt.show()

def sketch_lines(tick_bundle, value_trace_map, vertical_note, graph_heading, output_image):
    plt.figure(figsize=(8, 5))
    for stream in value_trace_map:
        plt.plot(tick_bundle[stream], value_trace_map[stream], marker='o', label=stream)

    plt.title(graph_heading)
    plt.xlabel("Input Size")
    plt.ylabel(vertical_note)
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_image)
    plt.show()

if __name__ == "__main__":
    stat_table = gather_performance_stats(performance_csv_path)

    sketch_bars(
        input_tracker={key: stat_table[key]['record_batch'] for key in stat_table},
        height_data_map={key: stat_table[key]['run_seconds'] for key in stat_table},
        y_axis_label="Execution Time (sec)",
        plot_caption="Execution Time vs Load",
        filename_for_save="time_vs_load.png"
    )

    sketch_lines(
        tick_bundle={key: stat_table[key]['record_batch'] for key in stat_table},
        value_trace_map={key: stat_table[key]['speed_rate'] for key in stat_table},
        vertical_note="Throughput (rec/sec)",
        graph_heading="Throughput vs Load",
        output_image="throughput_vs_load.png"
    )

    sketch_lines(
        tick_bundle={key: stat_table[key]['record_batch'] for key in stat_table},
        value_trace_map={key: stat_table[key]['lag_delay'] for key in stat_table},
        vertical_note="Latency (sec/rec)",
        graph_heading="Latency vs Load",
        output_image="latency_vs_load.png"
    )
