import paramiko
import psutil
import socket
import logging
from datetime import datetime
import json

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
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=f'server_health_check_{hostname}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
        )
        self.logger = logging.getLogger(__name__)

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
            services_to_check = ['ssh', 'nginx', 'apache2', 'mysql', 'postgresql']
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
            filename = f'health_report_{self.hostname}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(filename, 'w') as f:
                json.dump(report, f, indent=4)
            self.logger.info(f"Report saved to {filename}")
        except Exception as e:
            self.logger.error(f"Failed to save report: {e}")

def main():
    # Example usage
    checker = RemoteServerHealthChecker(
        hostname='10.32.209.18',  # Replace with your server hostname/IP
        username='root',  # Replace with your SSH username
        password='Reliance@2023',  # Use password OR key_filename
        # key_filename='/path/to/private/key'  # SSH key authentication alternative
    )

    try:
        # Perform health check
        health_report = checker.check_server_health()
        
        # Save report
        checker.save_report(health_report)
        
        # Print key highlights
        print(json.dumps(health_report, indent=2))
    
    except Exception as e:
        print(f"Error during health check: {e}")

if __name__ == '__main__':
    main()