
import pandas as pd
import matplotlib.pyplot as plt

csv_files = {'/home/nafra/Bureau/code/spark-handson/csvs/no_udf.csv' : 'no_udf', 
'/home/nafra/Bureau/code/spark-handson/csvs/python_udf.csv' : 'python_udf',
 '/home/nafra/Bureau/code/spark-handson/csvs/pandas_udf.csv' : 'pandas_udf', 
 '/home/nafra/Bureau/code/spark-handson/csvs/scala_udf.csv': 'scala_udf'}

def main():

 dfs = []
 for file, function_name in csv_files.items():
    df = pd.read_csv(file)
    df['function'] = function_name
    dfs.append(df)

 combined_df = pd.concat(dfs, ignore_index=True)
 fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(14, 15))
 metrics = ['read_time', 'op_time', 'write_time', 'avg_cpu_usage', 'peak_memory_usage']
 colors = ['skyblue', 'orange', 'green', 'red', 'purple']
 titles = ['Read Time', 'Operation Time', 'Write Time', 'Average CPU Usage', 'Peak Memory Usage']

 for i, metric in enumerate(metrics):
    ax = axes[i // 2, i % 2]
    combined_df.groupby('function')[metric].mean().plot(kind='bar', ax=ax, color=colors[i])
    ax.set_title(titles[i])
    ax.set_xlabel('Function')
    ax.set_ylabel(metric.replace('_', ' ').capitalize())

 axes[2, 1].axis('off')
 plt.tight_layout()
 plt.savefig('results/perf.png')
 print("plot saved")
