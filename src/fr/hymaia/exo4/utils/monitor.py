import threading
import psutil
import time 

class HardwareMonitor:
    def __init__(self, interval=1):
        self.interval = interval
        self.cpu_percentages = []
        self.memory_usages = []
        self._stop_event = threading.Event()

    def start(self):
        self._monitor_thread = threading.Thread(target=self._monitor)
        self._monitor_thread.start()

    def stop(self):
        self._stop_event.set()
        self._monitor_thread.join()

    def _monitor(self):
        while not self._stop_event.is_set():
            self.cpu_percentages.append(psutil.cpu_percent(interval=self.interval))
            self.memory_usages.append(psutil.virtual_memory().used / (1024 ** 3))  # GB
            time.sleep(self.interval)

    def get_avg_cpu(self):
        return sum(self.cpu_percentages) / len(self.cpu_percentages) if self.cpu_percentages else 0

    def get_avg_memory(self):
        return sum(self.memory_usages) / len(self.memory_usages) if self.memory_usages else 0

    def get_peak_memory(self):
        return max(self.memory_usages) if self.memory_usages else 0

