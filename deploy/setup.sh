#!/bin/bash
# AWS EC2/Lightsail Setup Script for DSR Flask App

# Update system
# For Amazon Linux 2023
sudo dnf update -y
sudo dnf install -y python3 python3-pip nginx git

# Create app directory
sudo mkdir -p /var/www/dsr
sudo chown -R ec2-user:ec2-user /var/www/dsr

# Clone the repository (or copy files)
cd /var/www/dsr
git clone https://github.com/akhil28tayal-pixel/DSR.git .

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Create uploads directory
mkdir -p uploads

# Set permissions
sudo chown -R ec2-user:ec2-user /var/www/dsr

echo "Setup complete! Now run: sudo cp deploy/dsr.service /etc/systemd/system/"
echo "Then: sudo systemctl enable dsr && sudo systemctl start dsr"
echo "Then: sudo cp deploy/nginx.conf /etc/nginx/sites-available/dsr"
echo "Then: sudo ln -s /etc/nginx/sites-available/dsr /etc/nginx/sites-enabled/"
echo "Then: sudo nginx -t && sudo systemctl restart nginx"
