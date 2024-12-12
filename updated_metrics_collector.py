import subprocess
import os
import csv
import concurrent.futures
import paramiko
import pandas as pd
import logging
from datetime import datetime

class MultiServerMetricsCollector:
    def __init__(self, credentials_file, output_file='multi_server_metrics.csv', log_file='multi_server_metrics.log'):
        """
        Initialize multi-server metrics collector
        
        :param credentials_file: CSV file with server credentials
        :param output_file: Output CSV file for metrics
        :param log_file: Logging file
        """
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=log_file,
            filemode='a'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load server credentials
        try:
            self.servers = pd.read_csv(credentials_file)
        except Exception as e:
            self.logger.error(f"Error reading credentials file: {e}")
            raise

        # Output file
        self.output_file = output_file
        
        # Initialize output CSV
        self._initialize_output_csv()

    def _initialize_output_csv(self):
        """
        Create output CSV with headers
        """
        try:
            headers = [
                'Timestamp', 'IP', 'Hostname', 
                'Memory_Usage', 'CPU_Usage', 'Disk_Usage', 
                'Load_Average', 'CPU_Min', 'CPU_Max', 'CPU_Average', 
                'Base_Frequency', 'Lowest_Frequency', 'Highest_Frequency', 
                'CPU_Vendor', 'Time_Sync_Status', 'Docker_Status', 
                'Connection_Status'
            ]
            
            with open(self.output_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
        except Exception as e:
            self.logger.error(f"Error initializing output CSV: {e}")
            raise

    def collect_server_metrics(self, ip, username, password):
        """
        Collect metrics for a single server
        
        :param ip: Server IP address
        :param username: SSH username
        :param password: SSH password
        :return: Dictionary of metrics
        """
        metrics = {
            'timestamp': datetime.now().isoformat(),
            'ip': ip,
            'connection_status': 'Failed'
        }

        try:
            # Establish SSH connection
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(ip, username=username, password=password)
            metrics['connection_status'] = 'Success'

            # Collect metrics via SSH commands
            def run_ssh_command(command):
                try:
                    _, stdout, _ = ssh.exec_command(command)
                    return stdout.read().decode('utf-8').strip()
                except Exception as e:
                    self.logger.warning(f"Command error on {ip}: {command} - {e}")
                    return "ERROR"

            # Hostname
            metrics['hostname'] = run_ssh_command('hostname')

            # Memory Usage
            metrics['memory_usage'] = run_ssh_command(
                "free -m | grep Mem | awk '{ printf(\"%.2f\", (($3/$2)) * 100) }'"
            )

            # CPU Usage
            metrics['cpu_usage'] = run_ssh_command(
                "top -bn1 | grep 'Cpu(s)' | awk '{print 100 - $8\"%\"}'"
            )

            # Disk Usage
            metrics['disk_usage'] = run_ssh_command(
                "df -h / | awk 'NR==2 {print $5}'"
            )

            # Load Average
            metrics['load_average'] = run_ssh_command(
                "cat /proc/loadavg | awk '{print $1}'"
            )

            # CPU Min/Max/Average (SAR)
            sar_data = run_ssh_command(
                "sar -f /var/log/sa/sa$(date -d 'yesterday' +%d) -s $(date +%T) | "
                "awk '!/Average|%system|Linux|RESTART|^$/ {print 100-$9}' && "
                "sar -f /var/log/sa/sa$(date -d 'today' +%d) | "
                "awk '!/Average|%system|Linux|RESTART|^$/ {print 100-$9}' | "
                "sort -nr"
            ).split('\n')
            
            try:
                metrics['cpu_min'] = f"{sar_data[-1]}%"
                metrics['cpu_max'] = f"{sar_data[0]}%"
                metrics['cpu_average'] = f"{sum(float(x) for x in sar_data)/len(sar_data):.2f}%"
            except Exception:
                metrics['cpu_min'] = metrics['cpu_max'] = metrics['cpu_average'] = "ERROR"

            # CPU Frequency
            metrics['base_frequency'] = run_ssh_command(
                "sudo dmidecode -t processor | grep -i 'Current Speed' | head -n1 | awk '{print $3 $4}' | tr -d 'MHz'"
            )

            # Lowest and Highest Core Frequencies
            frequencies = run_ssh_command(
                "cat /proc/cpuinfo | grep -i Mhz | awk -F ':' '{print $2}' | "
                "awk -F '.' '{print $1}' | tr -d ' ' | sort -n"
            ).split('\n')
            metrics['lowest_frequency'] = frequencies[0] if frequencies else "ERROR"
            metrics['highest_frequency'] = frequencies[-1] if frequencies else "ERROR"

            # CPU Vendor
            metrics['cpu_vendor'] = run_ssh_command(
                "cat /proc/cpuinfo | grep -i vendor | head -1 | awk '{print $3}'"
            )

            # Time Synchronization Status
            metrics['time_sync_status'] = run_ssh_command(
                "timedatectl | grep -i synchronized | awk -F ':' '{print $2}'"
            )

            # Docker Daemon Status
            metrics['docker_status'] = run_ssh_command(
                "systemctl is-active docker"
            )

            ssh.close()
            return metrics

        except paramiko.AuthenticationException:
            metrics['connection_status'] = 'Authentication Failed'
            self.logger.error(f"Authentication failed for {ip}")
        except Exception as e:
            self.logger.error(f"Error collecting metrics for {ip}: {e}")
        
        return metrics

    def collect_all_metrics(self, max_workers=5):
        """
        Collect metrics for all servers
        
        :param max_workers: Maximum parallel connections
        """
        # Use ThreadPoolExecutor for parallel processing
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Prepare futures
            futures = {
                executor.submit(
                    self.collect_server_metrics, 
                    row['ip'], 
                    row['username'], 
                    row['password']
                ): row['ip'] for _, row in self.servers.iterrows()
            }

            # Collect and save results
            for future in concurrent.futures.as_completed(futures):
                server_ip = futures[future]
                try:
                    metrics = future.result()
                    self._save_metrics(metrics)
                except Exception as e:
                    self.logger.error(f"Error processing {server_ip}: {e}")

    def _save_metrics(self, metrics):
        """
        Save metrics to CSV
        
        :param metrics: Dictionary of metrics for a server
        """
        try:
            with open(self.output_file, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    metrics.get('timestamp', ''),
                    metrics.get('ip', ''),
                    metrics.get('hostname', ''),
                    metrics.get('memory_usage', ''),
                    metrics.get('cpu_usage', ''),
                    metrics.get('disk_usage', ''),
                    metrics.get('load_average', ''),
                    metrics.get('cpu_min', ''),
                    metrics.get('cpu_max', ''),
                    metrics.get('cpu_average', ''),
                    metrics.get('base_frequency', ''),
                    metrics.get('lowest_frequency', ''),
                    metrics.get('highest_frequency', ''),
                    metrics.get('cpu_vendor', ''),
                    metrics.get('time_sync_status', ''),
                    metrics.get('docker_status', ''),
                    metrics.get('connection_status', '')
                ])
            self.logger.info(f"Metrics logged for {metrics.get('ip', 'Unknown IP')}")
        except Exception as e:
            self.logger.error(f"Error saving metrics: {e}")

def main():
    try:
        # Path to credentials CSV
        credentials_file = 'servers.csv'
        
        # Create metrics collector
        collector = MultiServerMetricsCollector(credentials_file)
        
        # Collect metrics for all servers
        collector.collect_all_metrics()
        
        print(f"Metrics collection complete. Check {collector.output_file} for results.")
    
    except Exception as e:
        print(f"Error in main execution: {e}")

if __name__ == '__main__':
    main()