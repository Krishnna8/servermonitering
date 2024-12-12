import subprocess
import psutil
import os
import re
import json
import socket
import platform
from datetime import datetime
import argparse
import logging
import yaml
import threading
import queue
import time

class SystemMetricsCollector:
    def __init__(self, config_file=None, log_level=logging.INFO):
        """
        Initialize the advanced system metrics collector
        
        :param config_file: Path to YAML configuration file
        :param log_level: Logging level
        """
        # Configure logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('system_metrics.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Initialize metrics dictionary
        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'system': {},
            'hardware': {},
            'network': {},
            'services': {},
            'performance': {}
        }

        # Load configuration
        self.config = self._load_config(config_file)

    def _load_config(self, config_file):
        """
        Load configuration from YAML file
        
        :param config_file: Path to configuration file
        :return: Configuration dictionary
        """
        default_config = {
            'collectors': {
                'system': True,
                'hardware': True,
                'network': True,
                'services': True,
                'performance': True
            },
            'advanced_checks': {
                'network_ping_hosts': ['8.8.8.8', 'google.com'],
                'services_to_monitor': ['ssh', 'docker', 'nginx', 'mysql']
            }
        }

        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    user_config = yaml.safe_load(f)
                    # Merge default config with user config
                    default_config.update(user_config)
            except Exception as e:
                self.logger.warning(f"Error loading config file: {e}. Using default config.")
        
        return default_config

    def _run_command(self, command, shell=True):
        """
        Run a shell command with error handling
        
        :param command: Command to run
        :param shell: Use shell execution
        :return: Command output
        """
        try:
            result = subprocess.run(
                command, 
                shell=shell, 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            return result.stdout.strip() if result.stdout else result.stderr.strip()
        except subprocess.TimeoutExpired:
            self.logger.error(f"Command timed out: {command}")
            return None
        except Exception as e:
            self.logger.error(f"Error running command {command}: {e}")
            return None

    def collect_system_info(self):
        """
        Collect comprehensive system information
        """
        if not self.config['collectors']['system']:
            return

        try:
            self.metrics['system'] = {
                'os': {
                    'name': platform.system(),
                    'release': platform.release(),
                    'version': platform.version(),
                    'machine': platform.machine(),
                },
                'hostname': socket.gethostname(),
                'uptime': self._run_command('uptime -p'),
                'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
        except Exception as e:
            self.logger.error(f"Error collecting system info: {e}")

    def collect_hardware_metrics(self):
        """
        Collect detailed hardware metrics
        """
        if not self.config['collectors']['hardware']:
            return

        try:
            # Memory metrics
            mem = psutil.virtual_memory()
            self.metrics['hardware']['memory'] = {
                'total': mem.total / (1024 * 1024 * 1024),  # GB
                'available': mem.available / (1024 * 1024 * 1024),
                'used': mem.used / (1024 * 1024 * 1024),
                'percent': mem.percent
            }

            # CPU metrics
            cpu_freq = psutil.cpu_freq()
            self.metrics['hardware']['cpu'] = {
                'physical_cores': psutil.cpu_count(logical=False),
                'total_cores': psutil.cpu_count(logical=True),
                'current_frequency': cpu_freq.current,
                'min_frequency': cpu_freq.min,
                'max_frequency': cpu_freq.max,
                'usage_per_core': psutil.cpu_percent(interval=1, percpu=True)
            }

            # Disk metrics
            disk_partitions = psutil.disk_partitions()
            self.metrics['hardware']['disk'] = []
            for partition in disk_partitions:
                try:
                    usage = psutil.disk_usage(partition.mountpoint)
                    disk_info = {
                        'device': partition.device,
                        'mountpoint': partition.mountpoint,
                        'fstype': partition.fstype,
                        'total_size': usage.total / (1024 * 1024 * 1024),
                        'used': usage.used / (1024 * 1024 * 1024),
                        'free': usage.free / (1024 * 1024 * 1024),
                        'percent': usage.percent
                    }
                    self.metrics['hardware']['disk'].append(disk_info)
                except Exception as e:
                    self.logger.warning(f"Error getting disk usage for {partition.mountpoint}: {e}")

        except Exception as e:
            self.logger.error(f"Error collecting hardware metrics: {e}")

    def collect_network_metrics(self):
        """
        Collect network-related metrics
        """
        if not self.config['collectors']['network']:
            return

        try:
            # Network interfaces
            net_if_addrs = psutil.net_if_addrs()
            self.metrics['network']['interfaces'] = {}
            for interface, addresses in net_if_addrs.items():
                self.metrics['network']['interfaces'][interface] = [
                    {
                        'family': addr.family.name,
                        'address': addr.address,
                        'netmask': addr.netmask
                    } for addr in addresses
                ]

            # Network connectivity checks
            network_hosts = self.config['advanced_checks']['network_ping_hosts']
            self.metrics['network']['connectivity'] = {}
            for host in network_hosts:
                try:
                    response = subprocess.run(
                        ['ping', '-c', '4', host], 
                        capture_output=True, 
                        text=True, 
                        timeout=5
                    )
                    self.metrics['network']['connectivity'][host] = (
                        response.returncode == 0
                    )
                except Exception as e:
                    self.logger.warning(f"Ping check failed for {host}: {e}")
                    self.metrics['network']['connectivity'][host] = False

        except Exception as e:
            self.logger.error(f"Error collecting network metrics: {e}")

    def collect_service_status(self):
        """
        Check status of specified services
        """
        if not self.config['collectors']['services']:
            return

        try:
            services = self.config['advanced_checks']['services_to_monitor']
            self.metrics['services'] = {}
            
            for service in services:
                status = self._run_command(f'systemctl is-active {service}')
                self.metrics['services'][service] = status
        except Exception as e:
            self.logger.error(f"Error collecting service status: {e}")

    def collect_performance_metrics(self):
        """
        Collect system performance metrics
        """
        if not self.config['collectors']['performance']:
            return

        try:
            # Load average
            load_avg = os.getloadavg()
            self.metrics['performance']['load_average'] = {
                '1_min': load_avg[0],
                '5_min': load_avg[1],
                '15_min': load_avg[2]
            }

            # Process information
            self.metrics['performance']['processes'] = {
                'total': len(psutil.pids()),
                'running': len([p for p in psutil.process_iter() if p.status() == psutil.STATUS_RUNNING]),
                'top_cpu_consumers': []
            }

            # Top CPU consuming processes
            for proc in sorted(psutil.process_iter(['pid', 'name', 'cpu_percent']), 
                               key=lambda x: x.info['cpu_percent'], 
                               reverse=True)[:5]:
                self.metrics['performance']['processes']['top_cpu_consumers'].append({
                    'pid': proc.info['pid'],
                    'name': proc.info['name'],
                    'cpu_percent': proc.info['cpu_percent']
                })

        except Exception as e:
            self.logger.error(f"Error collecting performance metrics: {e}")

    def collect_all_metrics(self):
        """
        Collect all system metrics based on configuration
        """
        metric_collectors = [
            self.collect_system_info,
            self.collect_hardware_metrics,
            self.collect_network_metrics,
            self.collect_service_status,
            self.collect_performance_metrics
        ]

        # Use threading to potentially speed up collection
        threads = []
        for collector in metric_collectors:
            thread = threading.Thread(target=collector)
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        return self.metrics

    def save_metrics(self, output_format='json'):
        """
        Save collected metrics to file
        
        :param output_format: Output format (json or yaml)
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if output_format.lower() == 'json':
                filename = f"system_metrics_{timestamp}.json"
                with open(filename, 'w') as f:
                    json.dump(self.metrics, f, indent=4)
            elif output_format.lower() == 'yaml':
                filename = f"system_metrics_{timestamp}.yaml"
                with open(filename, 'w') as f:
                    yaml.dump(self.metrics, f, default_flow_style=False)
            
            self.logger.info(f"Metrics saved to {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")
            return None

def main():
    # Argument parsing
    parser = argparse.ArgumentParser(description='Advanced System Metrics Collector')
    parser.add_argument('-c', '--config', 
                        help='Path to configuration file', 
                        default=None)
    parser.add_argument('-f', '--format', 
                        choices=['json', 'yaml'], 
                        default='json', 
                        help='Output file format')
    parser.add_argument('-v', '--verbose', 
                        action='store_true', 
                        help='Enable verbose logging')
    
    args = parser.parse_args()

    # Set logging level
    log_level = logging.DEBUG if args.verbose else logging.INFO

    # Create metrics collector
    collector = SystemMetricsCollector(
        config_file=args.config, 
        log_level=log_level
    )
    
    # Collect metrics
    metrics = collector.collect_all_metrics()
    
    # Print metrics to console
    print(json.dumps(metrics, indent=2))
    
    # Save metrics to file
    collector.save_metrics(output_format=args.format)

if __name__ == '__main__':
    main()