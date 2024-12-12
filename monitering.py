import os
import psutil
import socket
import platform
import subprocess
import logging
from datetime import datetime
import yaml
import requests
import json
import threading
import time

class DevOpsMonitoringTool:
    def __init__(self, config_path='config.yaml'):
        """
        Initialize the DevOps Monitoring Tool
        
        :param config_path: Path to the configuration YAML file
        """
        # Set up logging
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s: %(message)s',
            filename='devops_monitor.log'
        )
        self.logger = logging.getLogger(__name__)
        
        # Load configuration
        try:
            with open(config_path, 'r') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            self.logger.error(f"Config file not found at {config_path}")
            self.config = {}
        
        # Default monitoring settings
        self.monitored_services = self.config.get('services', [])
        self.monitored_servers = self.config.get('servers', [])
        self.alert_threshold = self.config.get('alert_threshold', {
            'cpu_usage': 80,
            'memory_usage': 85,
            'disk_usage': 90
        })
        
        # Monitoring results storage
        self.monitoring_results = {}
    
    def get_system_info(self):
        """
        Collect comprehensive system information
        
        :return: Dictionary of system details
        """
        try:
            return {
                'os': platform.system(),
                'os_release': platform.release(),
                'hostname': socket.gethostname(),
                'processor': platform.processor(),
                'total_memory': psutil.virtual_memory().total / (1024 * 1024 * 1024),
                'total_disk': psutil.disk_usage('/').total / (1024 * 1024 * 1024)
            }
        except Exception as e:
            self.logger.error(f"Error collecting system info: {e}")
            return {}
    
    def check_service_status(self, service_name):
        """
        Check the status of a specific service
        
        :param service_name: Name of the service to check
        :return: Boolean indicating service status
        """
        try:
            # Works for systemd-based systems (Linux)
            result = subprocess.run(
                ['systemctl', 'is-active', service_name], 
                capture_output=True, 
                text=True
            )
            return result.stdout.strip() == 'active'
        except Exception as e:
            self.logger.error(f"Error checking service {service_name}: {e}")
            return False
    
    def monitor_resources(self):
        """
        Monitor system resources and log alerts
        """
        try:
            # CPU Usage
            cpu_usage = psutil.cpu_percent(interval=1)
            
            # Memory Usage
            memory = psutil.virtual_memory()
            memory_usage = memory.percent
            
            # Disk Usage
            disk = psutil.disk_usage('/')
            disk_usage = disk.percent
            
            # Store results
            self.monitoring_results = {
                'timestamp': datetime.now().isoformat(),
                'cpu_usage': cpu_usage,
                'memory_usage': memory_usage,
                'disk_usage': disk_usage
            }
            
            # Check and log alerts
            if cpu_usage > self.alert_threshold['cpu_usage']:
                self.logger.warning(f"HIGH CPU USAGE: {cpu_usage}%")
            
            if memory_usage > self.alert_threshold['memory_usage']:
                self.logger.warning(f"HIGH MEMORY USAGE: {memory_usage}%")
            
            if disk_usage > self.alert_threshold['disk_usage']:
                self.logger.warning(f"HIGH DISK USAGE: {disk_usage}%")
        
        except Exception as e:
            self.logger.error(f"Resource monitoring error: {e}")
    
    def ping_servers(self):
        """
        Ping monitored servers and check connectivity
        """
        server_status = {}
        for server in self.monitored_servers:
            try:
                response = subprocess.run(
                    ['ping', '-c', '4', server], 
                    capture_output=True, 
                    text=True
                )
                server_status[server] = response.returncode == 0
            except Exception as e:
                server_status[server] = False
                self.logger.error(f"Error pinging {server}: {e}")
        
        return server_status
    
    def send_alert(self, message):
        """
        Send alerts via multiple channels (Slack, Email, etc.)
        
        :param message: Alert message to send
        """
        # Slack webhook alert (configure in config)
        slack_webhook = self.config.get('slack_webhook')
        if slack_webhook:
            try:
                requests.post(slack_webhook, json={'text': message})
            except Exception as e:
                self.logger.error(f"Slack alert failed: {e}")
    
    def continuous_monitoring(self, interval=60):
        """
        Continuously monitor system resources
        
        :param interval: Monitoring interval in seconds
        """
        while True:
            self.monitor_resources()
            server_status = self.ping_servers()
            
            # Check service statuses
            for service in self.monitored_services:
                if not self.check_service_status(service):
                    alert_msg = f"ALERT: Service {service} is DOWN"
                    self.logger.critical(alert_msg)
                    self.send_alert(alert_msg)
            
            time.sleep(interval)
    
    def generate_report(self):
        """
        Generate a monitoring report
        
        :return: JSON formatted report
        """
        system_info = self.get_system_info()
        monitoring_data = {
            'system_info': system_info,
            'current_monitoring_results': self.monitoring_results
        }
        
        return json.dumps(monitoring_data, indent=2)
    
    def start_monitoring(self):
        """
        Start continuous monitoring in a separate thread
        """
        monitoring_thread = threading.Thread(
            target=self.continuous_monitoring, 
            daemon=True
        )
        monitoring_thread.start()

def main():
    # Example usage
    monitor = DevOpsMonitoringTool('config.yaml')
    
    # Start monitoring
    monitor.start_monitoring()
    
    # Generate initial report
    print(monitor.generate_report())
    
    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Monitoring stopped.")

if __name__ == "__main__":
    main()