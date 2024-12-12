import paramiko
import csv
import json
import logging
from datetime import datetime
import concurrent.futures
import os

class RemoteServerHealthChecker:
    def __init__(self, hostname, username, password=None, key_filename=None, port=22):
        """
        Initialize the RemoteServerHealthChecker.
        
        :param hostname: IP or hostname of the remote server
        :param username: SSH username
        :param password: SSH password (optional)
        :param key_filename: Path to SSH private key file (optional)
        :param port: SSH port (default 22)
        """
        self.hostname = hostname
        self.username = username
        self.password = password
        self.key_filename = key_filename
        self.port = port
        
        # Configure logging
        self.logger = self._setup_logger()

    def _setup_logger(self):
        """
        Set up logging for each server.
        
        :return: Configured logger
        """
        # Ensure logs directory exists
        os.makedirs('logs', exist_ok=True)
        
        logger = logging.getLogger(f'health_check_{self.hostname}')
        logger.setLevel(logging.INFO)
        
        # File handler
        file_handler = logging.FileHandler(
            f'logs/server_health_check_{self.hostname}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def connect(self):
        """
        Establish SSH connection to the remote server.
        
        :return: paramiko SSHClient instance
        """
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Attempt connection with either password or key
            if self.key_filename:
                client.connect(
                    hostname=self.hostname, 
                    username=self.username, 
                    key_filename=self.key_filename, 
                    port=self.port
                )
            else:
                client.connect(
                    hostname=self.hostname, 
                    username=self.username, 
                    password=self.password, 
                    port=self.port
                )
            
            self.logger.info(f"Successfully connected to {self.hostname}")
            return client
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            raise

    def run_remote_command(self, client, command):
        """
        Run a command on the remote server.
        
        :param client: SSH client
        :param command: Command to run
        :return: Command output
        """
        try:
            stdin, stdout, stderr = client.exec_command(command)
            output = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()
            
            if error:
                self.logger.warning(f"Command error: {error}")
            
            return output
        except Exception as e:
            self.logger.error(f"Command execution failed: {e}")
            return None

    def check_server_health(self):
        """
        Perform comprehensive health check on the remote server.
        
        :return: Dictionary with health check results
        """
        client = None
        health_report = {
            'timestamp': datetime.now().isoformat(),
            'hostname': self.hostname,
            'system': {},
            'resources': {},
            'services': {},
            'network': {}
        }

        try:
            # Establish SSH connection
            client = self.connect()

            # Collect system information
            health_report['system']['os'] = self.run_remote_command(client, 'uname -a')
            health_report['system']['uptime'] = self.run_remote_command(client, 'uptime')
            health_report['system']['kernel'] = self.run_remote_command(client, 'uname -r')

            # Check disk usage
            disk_usage = self.run_remote_command(client, 'df -h')
            health_report['resources']['disk'] = disk_usage.split('\n')

            # Check memory usage
            memory_usage = self.run_remote_command(client, 'free -h')
            health_report['resources']['memory'] = memory_usage.split('\n')

            # Check CPU usage
            cpu_usage = self.run_remote_command(client, 'top -bn1 | grep "Cpu(s)"')
            health_report['resources']['cpu'] = cpu_usage

            # Check running processes
            processes = self.run_remote_command(client, 'ps aux | head -n 10')
            health_report['system']['top_processes'] = processes.split('\n')

            # Check critical services (customize as needed)
            services_to_check = ['ssh', 'nginx', 'apache2', 'mysql', 'postgresql', 'docker']
            health_report['services'] = {}
            for service in services_to_check:
                service_status = self.run_remote_command(client, f'systemctl is-active {service}')
                health_report['services'][service] = service_status

            # Network connectivity check
            health_report['network']['interfaces'] = self.run_remote_command(client, 'ip addr')
            health_report['network']['routes'] = self.run_remote_command(client, 'ip route')

            # Log successful health check
            self.logger.info(f"Health check completed for {self.hostname}")

            return health_report

        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            health_report['error'] = str(e)
            return health_report
        finally:
            if client:
                client.close()

    def save_report(self, report):
        """
        Save health check report to a JSON file.
        
        :param report: Health check report dictionary
        """
        try:
            # Ensure reports directory exists
            os.makedirs('reports', exist_ok=True)
            
            filename = f'reports/health_report_{self.hostname}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(filename, 'w') as f:
                json.dump(report, f, indent=4)
            self.logger.info(f"Report saved to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save report: {e}")

def process_server(server_details):
    """
    Process health check for a single server.
    
    :param server_details: Dictionary with server connection details
    :return: Health report for the server
    """
    try:
        # Create checker instance
        checker = RemoteServerHealthChecker(
            hostname=server_details['ip'],
            username=server_details['username'],
            password=server_details['password']
        )
        
        # Perform health check
        health_report = checker.check_server_health()
        
        # Save report
        checker.save_report(health_report)
        
        return health_report
    except Exception as e:
        print(f"Error processing server {server_details['ip']}: {e}")
        return None

def read_servers_from_csv(filename):
    """
    Read server details from a CSV file.
    
    :param filename: Path to the CSV file
    :return: List of server dictionaries
    """
    servers = []
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                servers.append(row)
        return servers
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return []

def main():
    # Path to your CSV file
    csv_file = 'servers.csv'
    
    # Read server details from CSV
    servers = read_servers_from_csv(csv_file)
    # print(servers)
    
    if not servers:
        print("No servers found in the CSV file.")
        return
    
    # Use concurrent processing to check multiple servers simultaneously
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit health checks for all servers
        future_to_server = {
            executor.submit(process_server, server): server 
            for server in servers
        }
        
        # Collect results
        for future in concurrent.futures.as_completed(future_to_server):
            server = future_to_server[future]
            try:
                result = future.result()
                if result:
                    print(f"Health check completed for {server['ip']}")
                else:
                    print(f"Health check failed for {server['ip']}")
            except Exception as e:
                print(f"Error processing {server['ip']}: {e}")

if __name__ == '__main__':
    main()